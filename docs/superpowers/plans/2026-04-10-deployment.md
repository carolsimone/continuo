# Deployment — Hetzner Dev Environment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy all 8 continuo services to a single Hetzner k3s server using two independent Helm charts (infra and app) with GitHub Actions CI/CD pushing images to Docker Hub on every push to `main`.

**Architecture:** Two Helm charts — `deploy/infra/` for stateful infrastructure (Postgres, Redis, Neo4j) deployed once via Bitnami/neo4j community charts, and `deploy/app/` for all 8 application services sharing a single iterable Deployment template. Flyway migrations run as a pre-install k8s Job via a custom migration image. GitHub Actions detects changed services by path, builds and pushes Docker Hub images tagged with git SHA, then SSHes into the Hetzner server to run `helm upgrade`.

**Tech Stack:** k3s, Helm 3, Bitnami postgresql chart, Bitnami redis chart, official neo4j Helm chart, hcloud-csi-driver, Traefik (bundled with k3s), GitHub Actions, Docker Hub, Hetzner Object Storage (S3-compatible), Flyway 10.

---

## File Map

```
deploy/
  infra/
    Chart.yaml
    values.yaml
  app/
    Chart.yaml
    values.yaml
    values.secret.yaml.example
    templates/
      _helpers.tpl
      secret.yaml
      configmap.yaml
      rbac.yaml
      deployment.yaml
      service.yaml
      ingress.yaml
      migrations.yaml
db/
  Dockerfile.migrate
  migrate-all.sh
graph/
  Dockerfile.prod              ← new
manifest-controller/
  Dockerfile.prod              ← new
config/
  schedules.yaml               ← existing, used as ConfigMap
.github/
  workflows/
    deploy.yml
.gitignore                     ← add deploy/app/values.secret.yaml
```

---

## Task 1: Create production Dockerfiles for graph and manifest-controller

**Files:**
- Create: `graph/Dockerfile.prod`
- Create: `manifest-controller/Dockerfile.prod`

- [ ] **Step 1: Write graph/Dockerfile.prod**

  Model it after `state/Dockerfile.prod` (same monorepo Go multi-stage pattern). Binary is `graph`, ports 8081 and 50052.

  ```dockerfile
  # graph/Dockerfile.prod
  FROM continuo-base:latest AS builder

  WORKDIR /app

  COPY go.work go.work.sum* ./
  COPY state/go.mod state/go.sum ./state/
  COPY pkg/go.mod pkg/go.sum ./pkg/
  COPY graph/go.mod graph/go.sum ./graph/
  COPY startup-controller/go.mod startup-controller/go.sum ./startup-controller/
  COPY executor-controller/go.mod executor-controller/go.sum ./executor-controller/
  COPY dependency-controller/go.mod dependency-controller/go.sum ./dependency-controller/
  COPY k8s-controller/go.mod k8s-controller/go.sum ./k8s-controller/
  COPY e2e/go.mod e2e/go.sum ./e2e/

  RUN --mount=type=cache,target=/go/pkg/mod \
      go mod download

  COPY graph/ ./graph/
  COPY pkg/ ./pkg/

  WORKDIR /app/graph
  RUN --mount=type=cache,target=/root/.cache/go-build \
      CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app/bin/graph .

  FROM alpine:latest
  RUN apk --no-cache add ca-certificates
  COPY --from=builder /app/bin/graph /usr/local/bin/graph
  EXPOSE 8081 50052
  CMD ["graph"]
  ```

- [ ] **Step 2: Write manifest-controller/Dockerfile.prod**

  Python 3.12 / uv multi-stage build. Installs only non-dev dependencies.

  ```dockerfile
  # manifest-controller/Dockerfile.prod
  FROM python:3.12-slim AS builder

  WORKDIR /app

  RUN pip install uv

  COPY pyproject.toml uv.lock ./
  RUN uv sync --frozen --no-dev

  FROM python:3.12-slim

  WORKDIR /app

  RUN pip install uv

  COPY --from=builder /app/.venv /app/.venv
  COPY pyproject.toml uv.lock ./
  COPY . .

  ENV PATH="/app/.venv/bin:$PATH"

  CMD ["python", "main.py"]
  ```

- [ ] **Step 3: Build graph prod image locally to verify it compiles**

  ```bash
  DOCKER_BUILDKIT=1 docker build -t continuo-graph:prod-test -f graph/Dockerfile.prod .
  ```

  Expected: image builds successfully, no compilation errors.

- [ ] **Step 4: Build manifest-controller prod image locally to verify**

  ```bash
  DOCKER_BUILDKIT=1 docker build -t continuo-manifest-controller:prod-test -f manifest-controller/Dockerfile.prod manifest-controller/
  ```

  Expected: image builds successfully.

- [ ] **Step 5: Commit**

  ```bash
  git add graph/Dockerfile.prod manifest-controller/Dockerfile.prod
  git commit -m "feat(deploy): add production Dockerfiles for graph and manifest-controller"
  ```

---

## Task 2: Create the DB migration image

**Files:**
- Create: `db/migrate-all.sh`
- Create: `db/Dockerfile.migrate`

Flyway cannot run multiple database migrations in a single invocation. This custom image bakes all SQL files in and runs them sequentially via a shell script.

- [ ] **Step 1: Write db/migrate-all.sh**

  ```bash
  #!/bin/sh
  set -e

  FLYWAY_OPTS="-connectRetries=30 -baselineOnMigrate=true"

  for db in state startup executor dependency k8s; do
    echo "Running migrations for continuo_${db}..."
    flyway \
      -url="jdbc:postgresql://${POSTGRES_HOST}:5432/continuo_${db}" \
      -user="${POSTGRES_USER}" \
      -password="${POSTGRES_PASSWORD}" \
      -locations="filesystem:/flyway/migrations/${db}" \
      ${FLYWAY_OPTS} \
      migrate
  done

  echo "All migrations complete."
  ```

- [ ] **Step 2: Write db/Dockerfile.migrate**

  ```dockerfile
  FROM flyway/flyway:10

  USER root
  COPY migrate-all.sh /migrate-all.sh
  RUN chmod +x /migrate-all.sh

  COPY migration/state    /flyway/migrations/state
  COPY migration/startup  /flyway/migrations/startup
  COPY migration/executor /flyway/migrations/executor
  COPY migration/dependency /flyway/migrations/dependency
  COPY migration/k8s      /flyway/migrations/k8s

  USER flyway
  ENTRYPOINT ["/bin/sh", "/migrate-all.sh"]
  ```

- [ ] **Step 3: Build migration image locally to verify**

  ```bash
  DOCKER_BUILDKIT=1 docker build -t continuo-migrations:test -f db/Dockerfile.migrate db/
  ```

  Expected: image builds, all SQL files are present inside the image.

- [ ] **Step 4: Commit**

  ```bash
  git add db/migrate-all.sh db/Dockerfile.migrate
  git commit -m "feat(deploy): add Flyway migration image for k8s pre-install job"
  ```

---

## Task 3: Hetzner server setup — k3s + hcloud-csi-driver

Run all commands via SSH on the Hetzner server.

- [ ] **Step 1: Install k3s (single-node, with Traefik enabled)**

  ```bash
  ssh root@<HETZNER_IP>

  curl -sfL https://get.k3s.io | sh -
  ```

  Expected: k3s installs, kubeconfig written to `/etc/rancher/k3s/k3s.yaml`.

- [ ] **Step 2: Verify cluster is healthy**

  ```bash
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  kubectl get nodes
  ```

  Expected: one node in `Ready` state.

- [ ] **Step 3: Install hcloud-cloud-controller-manager**

  Replace `<HCLOUD_TOKEN>` with a Hetzner API token (read+write scope).

  ```bash
  kubectl -n kube-system create secret generic hcloud \
    --from-literal=token=<HCLOUD_TOKEN> \
    --from-literal=network=<NETWORK_NAME>

  kubectl apply -f \
    https://github.com/hetznercloud/hcloud-cloud-controller-manager/releases/latest/download/ccm.yaml
  ```

- [ ] **Step 4: Install hcloud-csi-driver**

  ```bash
  kubectl -n kube-system create secret generic hcloud-csi \
    --from-literal=token=<HCLOUD_TOKEN>

  kubectl apply -f \
    https://raw.githubusercontent.com/hetznercloud/csi-driver/main/deploy/kubernetes/hcloud-csi.yml
  ```

- [ ] **Step 5: Verify StorageClass is available**

  ```bash
  kubectl get storageclass
  ```

  Expected: `hcloud-volumes` StorageClass listed.

- [ ] **Step 6: Create the continuo namespace**

  ```bash
  kubectl create namespace continuo
  ```

- [ ] **Step 7: Install Helm on the server**

  ```bash
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  ```

  Expected: `helm version` outputs Helm 3.x.

---

## Task 4: Infrastructure Helm chart

**Files:**
- Create: `deploy/infra/Chart.yaml`
- Create: `deploy/infra/values.yaml`

- [ ] **Step 1: Write deploy/infra/Chart.yaml**

  ```yaml
  apiVersion: v2
  name: continuo-infra
  description: Infrastructure dependencies for continuo (Postgres, Redis, Neo4j)
  type: application
  version: 0.1.0
  dependencies:
    - name: postgresql
      version: "^16.0.0"
      repository: oci://registry-1.docker.io/bitnamicharts
    - name: redis
      version: "^20.0.0"
      repository: oci://registry-1.docker.io/bitnamicharts
    - name: neo4j
      version: "^5.0.0"
      repository: https://helm.neo4j.com/neo4j
  ```

- [ ] **Step 2: Write deploy/infra/values.yaml**

  The initdb script creates all 6 databases in one Postgres instance. Passwords are placeholders — real values go in `values.secret.yaml` when deploying.

  ```yaml
  postgresql:
    auth:
      username: runner
      password: changeme
      postgresPassword: changeme
    primary:
      initdb:
        scripts:
          init-databases.sh: |
            #!/bin/bash
            set -e
            psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
                CREATE DATABASE continuo_state;
                CREATE DATABASE continuo_startup;
                CREATE DATABASE continuo_executor;
                CREATE DATABASE continuo_dependency;
                CREATE DATABASE continuo_k8s;
                CREATE DATABASE continuo_dbt;
                GRANT ALL PRIVILEGES ON DATABASE continuo_state TO $POSTGRES_USER;
                GRANT ALL PRIVILEGES ON DATABASE continuo_startup TO $POSTGRES_USER;
                GRANT ALL PRIVILEGES ON DATABASE continuo_executor TO $POSTGRES_USER;
                GRANT ALL PRIVILEGES ON DATABASE continuo_dependency TO $POSTGRES_USER;
                GRANT ALL PRIVILEGES ON DATABASE continuo_k8s TO $POSTGRES_USER;
                GRANT ALL PRIVILEGES ON DATABASE continuo_dbt TO $POSTGRES_USER;
            EOSQL
      persistence:
        storageClass: hcloud-volumes
        size: 10Gi
    sslmode: disable

  redis:
    auth:
      enabled: true
      password: changeme
    master:
      persistence:
        storageClass: hcloud-volumes
        size: 2Gi

  neo4j:
    neo4j:
      password: changeme
    env:
      NEO4J_dbms_memory_pagecache_size: "512M"
      NEO4J_dbms_memory_heap_initial__size: "512M"
      NEO4J_dbms_memory_heap_max__size: "1G"
    volumes:
      data:
        storageClassName: hcloud-volumes
        requests:
          storage: 10Gi
  ```

- [ ] **Step 3: Add Helm neo4j chart repo and pull dependencies**

  Run locally (with kubeconfig pointing at Hetzner server):

  ```bash
  helm repo add neo4j https://helm.neo4j.com/neo4j
  helm repo update
  helm dep update deploy/infra/
  ```

  Expected: `charts/` directory populated under `deploy/infra/`.

- [ ] **Step 4: Lint the infra chart**

  ```bash
  helm lint deploy/infra/
  ```

  Expected: `1 chart(s) linted, 0 chart(s) failed`.

- [ ] **Step 5: Deploy infra chart to the server**

  Create `deploy/infra/values.secret.yaml` (not committed):

  ```yaml
  postgresql:
    auth:
      password: <strong-password>
      postgresPassword: <strong-password>
  redis:
    auth:
      password: <strong-password>
  neo4j:
    neo4j:
      password: <strong-password>
  ```

  Deploy:

  ```bash
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

  helm install continuo-infra deploy/infra/ \
    -f deploy/infra/values.yaml \
    -f deploy/infra/values.secret.yaml \
    -n continuo \
    --wait --timeout 10m
  ```

  Expected: all infra pods Running.

- [ ] **Step 6: Verify service names (critical for app chart)**

  ```bash
  kubectl get svc -n continuo
  ```

  Expected output includes (note exact names for use in app chart):
  - `continuo-infra-postgresql`
  - `continuo-infra-redis-master`
  - `continuo-infra-neo4j` (or similar — update `deploy/app/values.yaml` if different)

- [ ] **Step 7: Commit chart files (exclude charts/ directory)**

  ```bash
  echo "deploy/infra/charts/" >> .gitignore
  echo "deploy/app/charts/" >> .gitignore
  git add deploy/infra/Chart.yaml deploy/infra/values.yaml .gitignore
  git commit -m "feat(deploy): add infra Helm chart (postgres, redis, neo4j)"
  ```

---

## Task 5: App chart — scaffold and secret template

**Files:**
- Create: `deploy/app/Chart.yaml`
- Create: `deploy/app/templates/_helpers.tpl`
- Create: `deploy/app/templates/secret.yaml`
- Create: `deploy/app/values.secret.yaml.example`
- Modify: `.gitignore`

- [ ] **Step 1: Write deploy/app/Chart.yaml**

  ```yaml
  apiVersion: v2
  name: continuo-app
  description: All continuo application services
  type: application
  version: 0.1.0
  ```

- [ ] **Step 2: Write deploy/app/templates/_helpers.tpl**

  ```
  {{/*
  Expand the name of the chart.
  */}}
  {{- define "app.name" -}}
  {{- .Chart.Name | trunc 63 | trimSuffix "-" }}
  {{- end }}

  {{/*
  Common labels applied to all resources.
  */}}
  {{- define "app.labels" -}}
  helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
  app.kubernetes.io/managed-by: {{ .Release.Service }}
  {{- end }}

  {{/*
  Selector labels for a given service name.
  */}}
  {{- define "app.selectorLabels" -}}
  app.kubernetes.io/name: {{ . }}
  {{- end }}
  ```

- [ ] **Step 3: Write deploy/app/templates/secret.yaml**

  Creates a single k8s Secret with all credentials. Services consume it via `envFrom.secretRef`.

  ```yaml
  apiVersion: v1
  kind: Secret
  metadata:
    name: continuo-credentials
    namespace: {{ .Release.Namespace }}
    labels:
      {{- include "app.labels" . | nindent 4 }}
  type: Opaque
  stringData:
    POSTGRES_USER: {{ .Values.global.postgres.user | quote }}
    POSTGRES_PASSWORD: {{ .Values.global.postgres.password | quote }}
    NEO4J_USER: {{ .Values.global.neo4j.user | quote }}
    NEO4J_PASSWORD: {{ .Values.global.neo4j.password | quote }}
    AWS_ACCESS_KEY_ID: {{ .Values.global.s3.accessKeyId | quote }}
    AWS_SECRET_ACCESS_KEY: {{ .Values.global.s3.secretKey | quote }}
  ```

- [ ] **Step 4: Write deploy/app/values.secret.yaml.example**

  This is the template developers copy and fill in. The real file is gitignored.

  ```yaml
  global:
    postgres:
      password: "changeme"
    neo4j:
      password: "changeme"
    s3:
      accessKeyId: "changeme"
      secretKey: "changeme"
  ```

- [ ] **Step 5: Gitignore the real secret file**

  ```bash
  echo "deploy/app/values.secret.yaml" >> .gitignore
  ```

- [ ] **Step 6: Lint**

  ```bash
  helm lint deploy/app/ --set global.imageTag=test \
    --set global.dockerHubUser=testuser \
    --set global.postgres.user=runner \
    --set global.postgres.password=x \
    --set global.neo4j.user=neo4j \
    --set global.neo4j.password=x \
    --set global.s3.accessKeyId=x \
    --set global.s3.secretKey=x
  ```

  Expected: `1 chart(s) linted, 0 chart(s) failed`.

- [ ] **Step 7: Commit**

  ```bash
  git add deploy/app/ .gitignore
  git commit -m "feat(deploy): scaffold app Helm chart with secret template"
  ```

---

## Task 6: App chart — ConfigMap template

**Files:**
- Create: `deploy/app/templates/configmap.yaml`

Shared non-secret config consumed by all services via `envFrom.configMapRef`. Secrets are separate (Task 5).

- [ ] **Step 1: Write deploy/app/templates/configmap.yaml**

  ```yaml
  apiVersion: v1
  kind: ConfigMap
  metadata:
    name: continuo-config
    namespace: {{ .Release.Namespace }}
    labels:
      {{- include "app.labels" . | nindent 4 }}
  data:
    POSTGRES_HOST: {{ .Values.global.postgres.host | quote }}
    POSTGRES_PORT: {{ .Values.global.postgres.port | quote }}
    POSTGRES_USER: {{ .Values.global.postgres.user | quote }}
    REDIS_HOST: {{ .Values.global.redis.host | quote }}
    REDIS_PORT: {{ .Values.global.redis.port | quote }}
    NEO4J_URI: {{ .Values.global.neo4j.uri | quote }}
    NEO4J_USER: {{ .Values.global.neo4j.user | quote }}
    STATE_SERVICE_GRPC_ADDR: {{ .Values.global.stateGrpcAddr | quote }}
    GRAPH_SERVICE_GRPC_ADDR: {{ .Values.global.graphGrpcAddr | quote }}
    S3_ENDPOINT_URL: {{ .Values.global.s3.endpointUrl | quote }}
    S3_BUCKET: {{ .Values.global.s3.bucket | quote }}
    S3_ENV: {{ .Values.global.s3.env | quote }}
    AWS_DEFAULT_REGION: {{ .Values.global.s3.region | quote }}
    ENV: {{ .Values.global.env | quote }}
    LOG_LEVEL: {{ .Values.global.logLevel | quote }}
  ```

- [ ] **Step 2: Verify rendering with helm template**

  ```bash
  helm template continuo-app deploy/app/ \
    --set global.imageTag=test \
    --set global.dockerHubUser=testuser \
    --set global.postgres.user=runner \
    --set global.postgres.password=x \
    --set global.postgres.host=continuo-infra-postgresql \
    --set global.postgres.port=5432 \
    --set global.redis.host=continuo-infra-redis-master \
    --set global.redis.port=6379 \
    --set global.neo4j.uri=bolt://continuo-infra-neo4j:7687 \
    --set global.neo4j.user=neo4j \
    --set global.neo4j.password=x \
    --set global.s3.endpointUrl=https://fsn1.your-objectstorage.com \
    --set global.s3.bucket=continuo \
    --set global.s3.env=dev \
    --set global.s3.region=eu-central-1 \
    --set global.s3.accessKeyId=x \
    --set global.s3.secretKey=x \
    --set global.stateGrpcAddr=state:50051 \
    --set global.graphGrpcAddr=graph:50052 \
    --set global.env=dev \
    --set global.logLevel=INFO \
    2>&1 | grep -A 30 "kind: ConfigMap"
  ```

  Expected: ConfigMap YAML with all keys populated, no `<nil>` values.

- [ ] **Step 3: Commit**

  ```bash
  git add deploy/app/templates/configmap.yaml
  git commit -m "feat(deploy): add shared ConfigMap template for app chart"
  ```

---

## Task 7: App chart — RBAC template

**Files:**
- Create: `deploy/app/templates/rbac.yaml`

`executor-controller` and `k8s-controller` need ServiceAccount, Role, and RoleBinding to call the Kubernetes API from inside the cluster. Other services get the default ServiceAccount.

- [ ] **Step 1: Write deploy/app/templates/rbac.yaml**

  ```yaml
  {{- range $name, $svc := .Values.services }}
  {{- if and $svc.rbac $svc.rbac.enabled }}
  ---
  apiVersion: v1
  kind: ServiceAccount
  metadata:
    name: {{ $name }}
    namespace: {{ $.Release.Namespace }}
    labels:
      {{- include "app.labels" $ | nindent 4 }}
  ---
  apiVersion: rbac.authorization.k8s.io/v1
  kind: Role
  metadata:
    name: {{ $name }}
    namespace: {{ $.Release.Namespace }}
    labels:
      {{- include "app.labels" $ | nindent 4 }}
  rules:
    {{- toYaml $svc.rbac.rules | nindent 2 }}
  ---
  apiVersion: rbac.authorization.k8s.io/v1
  kind: RoleBinding
  metadata:
    name: {{ $name }}
    namespace: {{ $.Release.Namespace }}
    labels:
      {{- include "app.labels" $ | nindent 4 }}
  roleRef:
    apiGroup: rbac.authorization.k8s.io
    kind: Role
    name: {{ $name }}
  subjects:
    - kind: ServiceAccount
      name: {{ $name }}
      namespace: {{ $.Release.Namespace }}
  {{- end }}
  {{- end }}
  ```

- [ ] **Step 2: Commit**

  ```bash
  git add deploy/app/templates/rbac.yaml
  git commit -m "feat(deploy): add RBAC template for executor-controller and k8s-controller"
  ```

---

## Task 8: App chart — Deployment template

**Files:**
- Create: `deploy/app/templates/deployment.yaml`

One template iterates the `services` map. Handles: services with no HTTP port (manifest-controller), services with gRPC port (state, graph), services with RBAC ServiceAccount (executor-controller, k8s-controller), and schedules ConfigMap mount (state).

- [ ] **Step 1: Write deploy/app/templates/deployment.yaml**

  ```yaml
  {{- range $name, $svc := .Values.services }}
  {{- if ne $svc.enabled false }}
  ---
  apiVersion: apps/v1
  kind: Deployment
  metadata:
    name: {{ $name }}
    namespace: {{ $.Release.Namespace }}
    labels:
      {{- include "app.labels" $ | nindent 4 }}
      app.kubernetes.io/name: {{ $name }}
  spec:
    replicas: {{ $svc.replicas | default 1 }}
    selector:
      matchLabels:
        {{- include "app.selectorLabels" $name | nindent 6 }}
    template:
      metadata:
        labels:
          {{- include "app.selectorLabels" $name | nindent 8 }}
      spec:
        {{- if and $svc.rbac $svc.rbac.enabled }}
        serviceAccountName: {{ $name }}
        {{- end }}
        containers:
          - name: {{ $name }}
            image: {{ $.Values.global.dockerHubUser }}/continuo-{{ $name }}:{{ $.Values.global.imageTag }}
            imagePullPolicy: IfNotPresent
            envFrom:
              - configMapRef:
                  name: continuo-config
              - secretRef:
                  name: continuo-credentials
            {{- if $svc.env }}
            env:
              {{- range $key, $val := $svc.env }}
              - name: {{ $key }}
                value: {{ $val | quote }}
              {{- end }}
            {{- end }}
            {{- if $svc.port }}
            ports:
              - name: http
                containerPort: {{ $svc.port }}
              {{- if $svc.grpcPort }}
              - name: grpc
                containerPort: {{ $svc.grpcPort }}
              {{- end }}
            readinessProbe:
              httpGet:
                path: /health
                port: {{ $svc.port }}
              initialDelaySeconds: 10
              periodSeconds: 5
              failureThreshold: 3
            livenessProbe:
              httpGet:
                path: /health
                port: {{ $svc.port }}
              initialDelaySeconds: 15
              periodSeconds: 10
              failureThreshold: 3
            {{- else }}
            readinessProbe:
              exec:
                command: ["pgrep", "-f", "python"]
              initialDelaySeconds: 10
              periodSeconds: 10
            livenessProbe:
              exec:
                command: ["pgrep", "-f", "python"]
              initialDelaySeconds: 20
              periodSeconds: 30
            {{- end }}
            resources:
              requests:
                cpu: {{ ($svc.resources).cpu | default "100m" | quote }}
                memory: {{ ($svc.resources).memory | default "128Mi" | quote }}
            {{- if $svc.volumeMounts }}
            volumeMounts:
              {{- toYaml $svc.volumeMounts | nindent 12 }}
            {{- end }}
        {{- if $svc.volumes }}
        volumes:
          {{- toYaml $svc.volumes | nindent 8 }}
        {{- end }}
  {{- end }}
  {{- end }}
  ```

- [ ] **Step 2: Commit**

  ```bash
  git add deploy/app/templates/deployment.yaml
  git commit -m "feat(deploy): add iterable Deployment template for all services"
  ```

---

## Task 9: App chart — Service and Ingress templates

**Files:**
- Create: `deploy/app/templates/service.yaml`
- Create: `deploy/app/templates/ingress.yaml`

- [ ] **Step 1: Write deploy/app/templates/service.yaml**

  Services with no HTTP port (manifest-controller) get no Service resource.

  ```yaml
  {{- range $name, $svc := .Values.services }}
  {{- if and (ne $svc.enabled false) $svc.port }}
  ---
  apiVersion: v1
  kind: Service
  metadata:
    name: {{ $name }}
    namespace: {{ $.Release.Namespace }}
    labels:
      {{- include "app.labels" $ | nindent 4 }}
      app.kubernetes.io/name: {{ $name }}
  spec:
    selector:
      {{- include "app.selectorLabels" $name | nindent 4 }}
    ports:
      - name: http
        port: {{ $svc.port }}
        targetPort: {{ $svc.port }}
      {{- if $svc.grpcPort }}
      - name: grpc
        port: {{ $svc.grpcPort }}
        targetPort: {{ $svc.grpcPort }}
      {{- end }}
    type: ClusterIP
  {{- end }}
  {{- end }}
  ```

- [ ] **Step 2: Write deploy/app/templates/ingress.yaml**

  Only services with `ingress.enabled: true` get an Ingress. Traefik (bundled with k3s) handles TLS via Let's Encrypt automatically when the `cert-manager` or Traefik TLS resolver is configured.

  ```yaml
  {{- range $name, $svc := .Values.services }}
  {{- if and (ne $svc.enabled false) $svc.ingress $svc.ingress.enabled }}
  ---
  apiVersion: networking.k8s.io/v1
  kind: Ingress
  metadata:
    name: {{ $name }}
    namespace: {{ $.Release.Namespace }}
    labels:
      {{- include "app.labels" $ | nindent 4 }}
    annotations:
      traefik.ingress.kubernetes.io/router.entrypoints: websecure
      traefik.ingress.kubernetes.io/router.tls: "true"
      traefik.ingress.kubernetes.io/router.tls.certresolver: letsencrypt
  spec:
    rules:
      - host: {{ $svc.ingress.host | quote }}
        http:
          paths:
            - path: /
              pathType: Prefix
              backend:
                service:
                  name: {{ $name }}
                  port:
                    number: {{ $svc.port }}
    tls:
      - hosts:
          - {{ $svc.ingress.host | quote }}
        secretName: {{ $name }}-tls
  {{- end }}
  {{- end }}
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add deploy/app/templates/service.yaml deploy/app/templates/ingress.yaml
  git commit -m "feat(deploy): add Service and Ingress templates for app chart"
  ```

---

## Task 10: App chart — Flyway migration Job

**Files:**
- Create: `deploy/app/templates/migrations.yaml`

The Job runs as a Helm pre-install/pre-upgrade hook. It connects to Postgres and runs all 5 migration sets using the `continuo-migrations` image built in Task 2.

- [ ] **Step 1: Write deploy/app/templates/migrations.yaml**

  ```yaml
  apiVersion: batch/v1
  kind: Job
  metadata:
    name: db-migrate
    namespace: {{ .Release.Namespace }}
    labels:
      {{- include "app.labels" . | nindent 4 }}
    annotations:
      "helm.sh/hook": pre-install,pre-upgrade
      "helm.sh/hook-weight": "-5"
      "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
  spec:
    backoffLimit: 3
    template:
      spec:
        restartPolicy: OnFailure
        containers:
          - name: migrate
            image: {{ .Values.global.dockerHubUser }}/continuo-migrations:{{ .Values.global.imageTag }}
            imagePullPolicy: IfNotPresent
            env:
              - name: POSTGRES_HOST
                valueFrom:
                  configMapKeyRef:
                    name: continuo-config
                    key: POSTGRES_HOST
              - name: POSTGRES_USER
                valueFrom:
                  secretKeyRef:
                    name: continuo-credentials
                    key: POSTGRES_USER
              - name: POSTGRES_PASSWORD
                valueFrom:
                  secretKeyRef:
                    name: continuo-credentials
                    key: POSTGRES_PASSWORD
  ```

  **Note:** The migration Job uses direct `env` from configmap/secret (not `envFrom`) so it only has the env vars it actually needs.

- [ ] **Step 2: Commit**

  ```bash
  git add deploy/app/templates/migrations.yaml
  git commit -m "feat(deploy): add Flyway pre-install migration Job"
  ```

---

## Task 11: App chart — values.yaml with all 8 services

**Files:**
- Create: `deploy/app/values.yaml`

This is the main values file. All env vars are translated from `docker-compose.yml` to k3s ClusterIP service names. `KUBECONFIG` is omitted for `startup-controller`, `executor-controller`, `k8s-controller` — they use in-cluster ServiceAccount auth automatically via client-go.

- [ ] **Step 1: Write deploy/app/values.yaml**

  ```yaml
  global:
    imageTag: latest
    dockerHubUser: "youruser"   # ← update with your Docker Hub username

    env: dev
    logLevel: INFO

    postgres:
      host: continuo-infra-postgresql   # verify with: kubectl get svc -n continuo
      port: "5432"
      user: runner
      password: ""                       # set in values.secret.yaml

    redis:
      host: continuo-infra-redis-master  # verify with: kubectl get svc -n continuo
      port: "6379"

    neo4j:
      uri: bolt://continuo-infra-neo4j:7687   # verify service name
      user: neo4j
      password: ""                             # set in values.secret.yaml

    stateGrpcAddr: "state:50051"
    graphGrpcAddr: "graph:50052"

    s3:
      endpointUrl: "https://fsn1.your-objectstorage.com"   # ← update with your Hetzner region
      bucket: continuo
      env: dev
      region: eu-central-1
      accessKeyId: ""     # set in values.secret.yaml
      secretKey: ""       # set in values.secret.yaml

  services:

    state:
      port: 8082
      grpcPort: 50051
      env:
        POSTGRES_DB: continuo_state
        DB_SSLMODE: disable
        DB_POOL_SIZE: "10"
        DB_MAX_OVERFLOW: "20"
        HEALTH_PORT: "8082"
        GRPC_PORT: "50051"
        REDIS_ADDR: "continuo-infra-redis-master:6379"
        REDIS_STREAM_SCHEDULER_STARTED: "scheduler.started:v1"
        REDIS_STREAM_SCHEDULES_LOADED: "schedules.loaded:v1"
        SCHEDULES_CONFIG_PATH: /etc/continuo/schedules.yaml
      volumeMounts:
        - name: schedules
          mountPath: /etc/continuo
          readOnly: true
      volumes:
        - name: schedules
          configMap:
            name: continuo-schedules

    graph:
      port: 8081
      grpcPort: 50052
      env:
        GRPC_PORT: "50052"
        HEALTH_PORT: "8081"

    startup-controller:
      port: 8083
      env:
        REDIS_CONSUMER_STREAM: "scheduler.started:v1"
        REDIS_CONSUMER_GROUP: startup_controller_consumers
        REDIS_PRODUCER_STREAM: "query.model:v1"
        POSTGRES_DB: continuo_startup
        HTTP_PORT: "8083"

    executor-controller:
      port: 8084
      rbac:
        enabled: true
        rules:
          - apiGroups: ["batch"]
            resources: ["jobs"]
            verbs: ["create", "get", "list", "watch"]
      env:
        REDIS_CONSUMER_STREAM: "query.model:v1"
        REDIS_CONSUMER_RETRY_STREAM: "task.retry:v1"
        REDIS_CONSUMER_GROUP: executor_controller_consumers
        REDIS_PRODUCER_STREAM: "executor.deployed:v1"
        POSTGRES_DB: continuo_executor
        K8S_NAMESPACE: continuo
        DBT_POSTGRES_DB: continuo_dbt
        HTTP_PORT: "8084"

    k8s-controller:
      port: 8085
      rbac:
        enabled: true
        rules:
          - apiGroups: ["batch"]
            resources: ["jobs"]
            verbs: ["get", "list", "watch"]
          - apiGroups: [""]
            resources: ["pods", "pods/log"]
            verbs: ["get", "list", "watch"]
      env:
        REDIS_CONSUMER_DEPLOYED_STREAM: "executor.deployed:v1"
        REDIS_CONSUMER_CHECK_STREAM: "k8s.check:v1"
        REDIS_CONSUMER_GROUP: k8s_controller_consumers
        REDIS_PRODUCER_CHECK_STREAM: "k8s.check:v1"
        REDIS_PRODUCER_RETRY_STREAM: "task.retry:v1"
        REDIS_PRODUCER_FAILED_STREAM: "task.failed:v1"
        REDIS_PRODUCER_UPDATE_TABLE_STREAM: "update.table:v1"
        POSTGRES_DB: continuo_k8s
        K8S_NAMESPACE: continuo
        LOG_TAIL_LINES: "50"
        K8S_CHECK_DELAY_SECONDS: "30"
        ERROR_MESSAGE_MAX_LENGTH: "4096"
        HTTP_PORT: "8085"

    dependency-controller:
      port: 8086
      env:
        REDIS_CONSUMER_STREAM: "update.table:v1"
        REDIS_CONSUMER_GROUP: dependency_controller_consumers
        REDIS_PRODUCER_STREAM: "query.model:v1"
        POSTGRES_DB: continuo_dependency
        HTTP_PORT: "8086"

    ui-service:
      port: 8090
      ingress:
        enabled: true
        host: dev.yourdomain.com   # ← update with your domain
      env:
        PORT: "8090"
        NODE_ENV: production

    manifest-controller:
      port: null    # Redis consumer only, no HTTP port
      env:
        REDIS_URL: "redis://:$(REDIS_PASSWORD)@continuo-infra-redis-master:6379"
        GRAPH_GRPC_ADDR: "graph:50052"
        REGISTRY_PATH: /data/registry.csv
        MANIFESTS_BASE: /manifests
      # Note: /data needs a PVC for registry.csv persistence.
      # /manifests is a known limitation — see architecture note below.
      resources:
        cpu: 200m
        memory: 256Mi
  ```

  **Architecture note — manifest-controller volumes:** In the local docker-compose setup, `/manifests` is mounted from `./dbt/services/`. In k3s this directory is not available. Two options: (a) build manifests into the image at build time, or (b) run a `compile-and-upload` Job that uploads to S3 and update `MANIFESTS_BASE` to point to an S3 path. This is a follow-up task — for now the service deploys but won't process manifests until the volume is resolved.

- [ ] **Step 2: Create ConfigMap for schedules.yaml**

  The `state` service reads schedules from a mounted file. Create a template that renders `config/schedules.yaml` as a ConfigMap. Add this file:

  `deploy/app/templates/schedules-configmap.yaml`:

  ```yaml
  apiVersion: v1
  kind: ConfigMap
  metadata:
    name: continuo-schedules
    namespace: {{ .Release.Namespace }}
    labels:
      {{- include "app.labels" . | nindent 4 }}
  data:
    schedules.yaml: |
      {{- .Files.Get "../../config/schedules.yaml" | nindent 4 }}
  ```

  **Note:** Helm's `Files.Get` is relative to the chart directory. Since `config/schedules.yaml` is outside `deploy/app/`, copy it into the chart at deploy time, OR symlink it, OR embed the content directly in values.yaml. The simplest approach for a dev environment: copy `config/schedules.yaml` to `deploy/app/files/schedules.yaml` and reference it as `.Files.Get "files/schedules.yaml"`.

  Copy the file:

  ```bash
  mkdir -p deploy/app/files
  cp config/schedules.yaml deploy/app/files/schedules.yaml
  ```

  Update the template to use the local copy:

  ```yaml
  data:
    schedules.yaml: |
      {{- .Files.Get "files/schedules.yaml" | nindent 4 }}
  ```

- [ ] **Step 3: Run helm lint on full chart**

  ```bash
  helm lint deploy/app/ -f deploy/app/values.yaml \
    --set global.postgres.password=x \
    --set global.neo4j.password=x \
    --set global.s3.accessKeyId=x \
    --set global.s3.secretKey=x
  ```

  Expected: `1 chart(s) linted, 0 chart(s) failed`.

- [ ] **Step 4: Render and inspect all Deployments**

  ```bash
  helm template continuo-app deploy/app/ \
    -f deploy/app/values.yaml \
    --set global.postgres.password=x \
    --set global.neo4j.password=x \
    --set global.s3.accessKeyId=x \
    --set global.s3.secretKey=x \
    2>&1 | grep "^kind:" | sort | uniq -c
  ```

  Expected output (counts may vary):
  ```
  8 Deployment
  2 Role
  2 RoleBinding
  2 ServiceAccount
  7 Service          (manifest-controller has no Service)
  1 Ingress          (ui-service only)
  1 ConfigMap        (continuo-config)
  1 ConfigMap        (continuo-schedules)
  1 Secret
  1 Job              (db-migrate)
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add deploy/app/ config/
  git commit -m "feat(deploy): add full app Helm chart values for all 8 services"
  ```

---

## Task 12: Deploy app chart and verify all services

- [ ] **Step 1: Build and push the migration image to Docker Hub**

  ```bash
  docker login

  DOCKER_BUILDKIT=1 docker build -t <user>/continuo-migrations:latest -f db/Dockerfile.migrate db/
  docker push <user>/continuo-migrations:latest
  ```

- [ ] **Step 2: Build and push all service images**

  For each service (replace `<user>` with your Docker Hub username):

  ```bash
  # Go services with Dockerfile.prod
  for svc in state startup-controller executor-controller k8s-controller dependency-controller; do
    DOCKER_BUILDKIT=1 docker build -t <user>/continuo-${svc}:latest -f ${svc}/Dockerfile.prod .
    docker push <user>/continuo-${svc}:latest
  done

  # graph
  DOCKER_BUILDKIT=1 docker build -t <user>/continuo-graph:latest -f graph/Dockerfile.prod .
  docker push <user>/continuo-graph:latest

  # ui-service
  DOCKER_BUILDKIT=1 docker build -t <user>/continuo-ui-service:latest -f ui-service/Dockerfile ui-service/
  docker push <user>/continuo-ui-service:latest

  # manifest-controller
  DOCKER_BUILDKIT=1 docker build -t <user>/continuo-manifest-controller:latest -f manifest-controller/Dockerfile.prod manifest-controller/
  docker push <user>/continuo-manifest-controller:latest
  ```

- [ ] **Step 3: Create values.secret.yaml on the server**

  SSH into the Hetzner server and create `/root/continuo-values.secret.yaml`:

  ```yaml
  global:
    postgres:
      password: "<same password as infra chart>"
    neo4j:
      password: "<same password as infra chart>"
    s3:
      accessKeyId: "<Hetzner Object Storage access key>"
      secretKey: "<Hetzner Object Storage secret key>"
  ```

- [ ] **Step 4: Deploy the app chart**

  ```bash
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

  helm upgrade --install continuo-app /path/to/deploy/app/ \
    -f /path/to/deploy/app/values.yaml \
    -f /root/continuo-values.secret.yaml \
    --set global.dockerHubUser=<user> \
    --set global.imageTag=latest \
    -n continuo \
    --wait --timeout 5m
  ```

- [ ] **Step 5: Verify all pods are running**

  ```bash
  kubectl get pods -n continuo
  ```

  Expected: all 8 service pods and the migration Job show Running/Completed. If a pod is in `CrashLoopBackOff`, inspect logs:

  ```bash
  kubectl logs -n continuo deployment/<service-name> --previous
  ```

- [ ] **Step 6: Verify Traefik routes ui-service**

  After pointing a DNS A record for your domain to the Hetzner server IP:

  ```bash
  curl -I https://dev.yourdomain.com/
  ```

  Expected: HTTP 200 from the ui-service.

---

## Task 13: Configure Traefik Let's Encrypt resolver

Traefik is bundled with k3s but needs a TLS resolver configured for Let's Encrypt.

- [ ] **Step 1: Create Traefik config for Let's Encrypt**

  Create `/var/lib/rancher/k3s/server/manifests/traefik-config.yaml` on the server:

  ```yaml
  apiVersion: helm.cattle.io/v1
  kind: HelmChartConfig
  metadata:
    name: traefik
    namespace: kube-system
  spec:
    valuesContent: |-
      additionalArguments:
        - "--certificatesresolvers.letsencrypt.acme.email=<your-email>"
        - "--certificatesresolvers.letsencrypt.acme.storage=/data/acme.json"
        - "--certificatesresolvers.letsencrypt.acme.tlschallenge=true"
      persistence:
        enabled: true
        storageClass: hcloud-volumes
        size: 128Mi
  ```

  k3s automatically picks up files in `/var/lib/rancher/k3s/server/manifests/` and applies them.

- [ ] **Step 2: Verify Traefik restarts with new config**

  ```bash
  kubectl rollout status deployment/traefik -n kube-system --timeout=2m
  ```

---

## Task 14: GitHub Actions CI/CD

**Files:**
- Create: `.github/workflows/deploy.yml`

Three jobs: detect which services changed, build+push changed images, deploy via SSH.

- [ ] **Step 1: Add required secrets to the GitHub repo**

  In the repository Settings → Secrets → Actions, add:

  | Secret | Value |
  |--------|-------|
  | `DOCKERHUB_USERNAME` | Your Docker Hub username |
  | `DOCKERHUB_TOKEN` | Docker Hub access token (Settings → Security → Access Tokens) |
  | `HETZNER_HOST` | Your Hetzner server IP |
  | `HETZNER_SSH_KEY` | Private key content for SSH (the key's public half must be in `~/.ssh/authorized_keys` on the server) |

- [ ] **Step 2: Write .github/workflows/deploy.yml**

  ```yaml
  name: Build and Deploy

  on:
    push:
      branches: [main]

  env:
    REGISTRY: docker.io
    IMAGE_PREFIX: ${{ secrets.DOCKERHUB_USERNAME }}/continuo

  jobs:
    detect-changes:
      runs-on: ubuntu-latest
      outputs:
        state: ${{ steps.filter.outputs.state }}
        graph: ${{ steps.filter.outputs.graph }}
        startup-controller: ${{ steps.filter.outputs.startup-controller }}
        executor-controller: ${{ steps.filter.outputs.executor-controller }}
        k8s-controller: ${{ steps.filter.outputs.k8s-controller }}
        dependency-controller: ${{ steps.filter.outputs.dependency-controller }}
        ui-service: ${{ steps.filter.outputs.ui-service }}
        manifest-controller: ${{ steps.filter.outputs.manifest-controller }}
        migrations: ${{ steps.filter.outputs.migrations }}
      steps:
        - uses: actions/checkout@v4
        - uses: dorny/paths-filter@v3
          id: filter
          with:
            filters: |
              state:
                - 'state/**'
                - 'pkg/**'
              graph:
                - 'graph/**'
                - 'pkg/**'
              startup-controller:
                - 'startup-controller/**'
                - 'pkg/**'
              executor-controller:
                - 'executor-controller/**'
                - 'pkg/**'
              k8s-controller:
                - 'k8s-controller/**'
                - 'pkg/**'
              dependency-controller:
                - 'dependency-controller/**'
                - 'pkg/**'
              ui-service:
                - 'ui-service/**'
              manifest-controller:
                - 'manifest-controller/**'
              migrations:
                - 'db/**'

    build-push:
      needs: detect-changes
      runs-on: ubuntu-latest
      strategy:
        matrix:
          include:
            - service: state
              changed: ${{ needs.detect-changes.outputs.state }}
              dockerfile: state/Dockerfile.prod
              context: .
            - service: graph
              changed: ${{ needs.detect-changes.outputs.graph }}
              dockerfile: graph/Dockerfile.prod
              context: .
            - service: startup-controller
              changed: ${{ needs.detect-changes.outputs.startup-controller }}
              dockerfile: startup-controller/Dockerfile.prod
              context: .
            - service: executor-controller
              changed: ${{ needs.detect-changes.outputs.executor-controller }}
              dockerfile: executor-controller/Dockerfile.prod
              context: .
            - service: k8s-controller
              changed: ${{ needs.detect-changes.outputs.k8s-controller }}
              dockerfile: k8s-controller/Dockerfile.prod
              context: .
            - service: dependency-controller
              changed: ${{ needs.detect-changes.outputs.dependency-controller }}
              dockerfile: dependency-controller/Dockerfile.prod
              context: .
            - service: ui-service
              changed: ${{ needs.detect-changes.outputs.ui-service }}
              dockerfile: ui-service/Dockerfile
              context: ui-service
            - service: manifest-controller
              changed: ${{ needs.detect-changes.outputs.manifest-controller }}
              dockerfile: manifest-controller/Dockerfile.prod
              context: manifest-controller
            - service: migrations
              changed: ${{ needs.detect-changes.outputs.migrations }}
              dockerfile: db/Dockerfile.migrate
              context: db
      steps:
        - uses: actions/checkout@v4

        - name: Skip if unchanged
          if: matrix.changed != 'true'
          run: echo "No changes in ${{ matrix.service }}, skipping build."

        - name: Set up Docker Buildx
          if: matrix.changed == 'true'
          uses: docker/setup-buildx-action@v3

        - name: Log in to Docker Hub
          if: matrix.changed == 'true'
          uses: docker/login-action@v3
          with:
            username: ${{ secrets.DOCKERHUB_USERNAME }}
            password: ${{ secrets.DOCKERHUB_TOKEN }}

        - name: Build and push
          if: matrix.changed == 'true'
          uses: docker/build-push-action@v5
          with:
            context: ${{ matrix.context }}
            file: ${{ matrix.dockerfile }}
            push: true
            tags: |
              ${{ env.IMAGE_PREFIX }}-${{ matrix.service }}:${{ github.sha }}
              ${{ env.IMAGE_PREFIX }}-${{ matrix.service }}:latest
            cache-from: type=gha
            cache-to: type=gha,mode=max

    deploy:
      needs: build-push
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v4

        - name: Deploy via SSH
          uses: appleboy/ssh-action@v1
          with:
            host: ${{ secrets.HETZNER_HOST }}
            username: root
            key: ${{ secrets.HETZNER_SSH_KEY }}
            script: |
              export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

              helm upgrade --install continuo-app /root/deploy/app/ \
                -f /root/deploy/app/values.yaml \
                -f /root/continuo-values.secret.yaml \
                --set global.dockerHubUser=${{ secrets.DOCKERHUB_USERNAME }} \
                --set global.imageTag=${{ github.sha }} \
                -n continuo \
                --wait --timeout 5m
  ```

  **Note on deploy step:** The workflow expects the chart files to exist at `/root/deploy/app/` on the server. The simplest approach for a dev environment: clone the repo on the server and `git pull` before `helm upgrade`. Update the SSH script to:

  ```bash
  cd /root/continuo && git pull
  helm upgrade --install continuo-app /root/continuo/deploy/app/ ...
  ```

- [ ] **Step 3: Clone the repo on the server (one-time setup)**

  ```bash
  ssh root@<HETZNER_IP>
  git clone https://github.com/<user>/continuo.git /root/continuo
  ```

- [ ] **Step 4: Push to main and verify the workflow runs**

  ```bash
  git push origin main
  ```

  In GitHub Actions, verify:
  - `detect-changes` job completes
  - `build-push` matrix jobs run (only for changed services)
  - `deploy` job SSHes and runs helm upgrade
  - All pods stay Running after deploy

- [ ] **Step 5: Commit the workflow**

  ```bash
  git add .github/workflows/deploy.yml
  git commit -m "feat(deploy): add GitHub Actions CI/CD pipeline for Hetzner deployment"
  ```

---

## Self-Review Notes

**Spec coverage check:**
- ✅ Single Hetzner server, k3s single-node — Task 3
- ✅ deploy/infra/ (bitnami/postgresql, bitnami/redis, neo4j) — Task 4
- ✅ deploy/app/ with single iterable Deployment template — Tasks 5-11
- ✅ GitHub Actions: detect changes, build+push, SSH helm upgrade — Task 14
- ✅ Traefik ingress for ui-service — Tasks 9, 13
- ✅ hcloud-csi-driver for PVs — Task 3
- ✅ Hetzner Object Storage replaces LocalStack — values.yaml in Task 11
- ✅ values.secret.yaml gitignored — Task 5
- ✅ executor-controller + k8s-controller RBAC — Tasks 7, 11
- ✅ manifest-controller exec probe (no HTTP port) — Task 8
- ✅ Postgres 6 databases via initdb script — Task 4
- ✅ Images tagged with git SHA — Task 14
- ✅ graph/Dockerfile.prod + manifest-controller/Dockerfile.prod — Task 1
- ⚠️ manifest-controller /manifests volume — noted as follow-up in Task 11
- ⚠️ startup-controller KUBECONFIG removal — noted in values.yaml (env var simply omitted)
