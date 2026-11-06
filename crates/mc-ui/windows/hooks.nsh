; The server runs from the install dir (Scheduled Task), and Windows cannot replace
; or delete a running executable: stop it before files change, and drop the task
; on uninstall. Data, config and keyring secrets are left alone on purpose.
!macro NSIS_HOOK_PREINSTALL
  nsExec::Exec '"$INSTDIR\mail-cat-server.exe" service stop'
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  nsExec::Exec '"$INSTDIR\mail-cat-server.exe" service uninstall'
!macroend
