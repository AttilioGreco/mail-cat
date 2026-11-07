import { invoke } from '@tauri-apps/api/core';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

export interface ServerInfo {
  version: string;
  executablePath: string;
  processId: number;
  /** Unix seconds. */
  startedAt: number;
}

export interface ServerStatus {
  appVersion: string;
  /** `legacy`: reachable but too old to report anything about itself. */
  state: 'down' | 'legacy' | 'running';
  info: ServerInfo | null;
}

const KEY = ['server-status'];

export function useServerStatus() {
  return useQuery({
    queryKey: KEY,
    queryFn: () => invoke<ServerStatus>('server_status'),
    refetchInterval: 5000,
  });
}

/** Restarts the server onto the installed build and refreshes everything it served. */
export function useRestartServerNow() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => invoke<ServerStatus>('restart_server'),
    onSuccess: (status) => {
      queryClient.setQueryData(KEY, status);
      return queryClient.invalidateQueries();
    },
  });
}
