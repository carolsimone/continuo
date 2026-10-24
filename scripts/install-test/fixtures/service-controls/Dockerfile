FROM python:3.14-alpine
CMD ["python", "-u", "-c", "import socket; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(('0.0.0.0',50060)); s.listen(); exec('while True:\\n c,a=s.accept(); c.sendall(c.recv(1024)); c.close()')"]
