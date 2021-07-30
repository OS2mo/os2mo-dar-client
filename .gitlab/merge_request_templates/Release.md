## Author's checklist

- [ ] The release is contained within one commit which references the ticket ID (eg. `[#12345] Release x.y.z`)
- [ ] The title of this MR contains the relevant ticket no., formatted like `[#12345]` or `#12345`
- [ ] The release notes (``NEWS.md``) have been updated
- [ ] The project file (``pyproject.toml``) has been updated
- [ ] The init file (``ra_utils/__init__.py``) has been updated
- [ ] The corresponding Redmine ticket has been set to `Needs review`

## Post-merge checklist

- [ ] The merged commit has been tagged with the version number
- [ ] The release pipeline ran succesfully
