import { buildProjectColors } from './project-helpers';

// ProjectTiles shows the set of project images a release check executes with:
// one tile per project, sorted by name so a project sits in the same place on
// every release, carrying the accent buildProjectColors assigns it within this
// set, its name, and its image tag. The changed project — the one whose image
// is new in this release check — is marked, and the sub-line says so, naming
// what the page shows (subject: "release" unless the page says otherwise) and
// where every other image was carried over from (carriedFrom: "prod" for a
// candidate; the verified release for a fix's release check, whose set is
// that release's with the fixed project swapped in). Renders nothing when no
// image has been recorded yet.
export default function ProjectTiles({ imageTags, changedProject, subject = 'release', carriedFrom = 'prod' }: {
  imageTags: Record<string, string>;
  changedProject?: string;
  subject?: string;
  carriedFrom?: string;
}) {
  const names = Object.keys(imageTags).sort();
  if (names.length === 0) return null;
  const colors = buildProjectColors(names);
  const changed = changedProject && names.includes(changedProject) ? changedProject : '';
  const carried = names.length - 1;
  const sub = changed
    ? `${changed} is new in this ${subject}${carried > 0 ? ` · ${carried} carried over from ${carriedFrom}` : ''}`
    : '';
  return (
    <>
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Projects</span>
          <span className="section-header__count">{names.length}</span>
        </div>
        {sub && <div className="section-header__sub">{sub}</div>}
      </div>
      <div className="project-tile-grid">
        {names.map(name => (
          <div key={name} className={`project-tile${name === changed ? ' project-tile--changed' : ''}`}>
            <div className="project-tile__head">
              <span className="nodes-group-dot" style={{ background: colors.get(name) }} />
              <span className="project-tile__name">{name}</span>
              {name === changed && <span className="pill-sm pill-sm--changed">changed</span>}
            </div>
            <div className="project-tile__tag">{imageTags[name]}</div>
          </div>
        ))}
      </div>
    </>
  );
}
