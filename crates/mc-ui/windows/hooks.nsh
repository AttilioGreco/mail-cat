; The server runs from the install dir (Scheduled Task), and Windows cannot replace
; or delete a running executable. `service stop` only asks the task to stop and can
; return before the process is gone (or miss a server started outside the task), so
; whatever is left is killed. taskkill fails harmlessly when nothing runs, hence the
; ignored exit code. Data, config and keyring secrets are left alone on purpose.
!macro NSIS_HOOK_PREINSTALL
  nsExec::Exec '"$INSTDIR\mail-cat-server.exe" service stop'
  nsExec::Exec 'taskkill /F /T /IM mail-cat-server.exe'
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  nsExec::Exec '"$INSTDIR\mail-cat-server.exe" service uninstall'
  nsExec::Exec 'taskkill /F /T /IM mail-cat-server.exe'
!macroend
