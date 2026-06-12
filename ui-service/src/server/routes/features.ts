import { Router } from 'express';

// Exposes server-side feature toggles the client needs at runtime. The chat
// bridge is attached to the HTTP server only when CHAT_BRIDGE_ENABLED is set, so
// the client must read this flag to decide whether to open the socket and render
// the chat panel instead of showing a permanently disconnected one.
export function createFeaturesRouter(chatBridgeEnabled: boolean) {
  const router = Router();
  router.get('/', (_req, res) => {
    res.json({ chatBridgeEnabled });
  });
  return router;
}
