import { createContext, useContext } from 'react';
import type { AuthUser } from './useAuth';

export const AuthContext = createContext<AuthUser | null>(null);

export function useCurrentUser(): AuthUser | null {
  return useContext(AuthContext);
}
