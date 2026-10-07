import { Router } from 'express';

// Exposes server-side feature toggles the client needs at runtime. The chat
// bridge is attached to the HTTP server only when CHAT_BRIDGE_ENABLED is set, so
// the client must read this flag to decide whether to open the socket and render
// the chat panel instead of showing a permanently disconnected one. maintenance
// tells the client to show the maintenance banner.
export function createFeaturesRouter(chatBridgeEnabled: boolean, maintenance = false) {
  const router = Router();
  router.get('/', (_req, res) => {
    res.json({ chatBridgeEnabled, maintenance });
  });
  return router;
}
