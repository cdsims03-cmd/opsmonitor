# Ops Monitor Deployment Notes

## Quick deploy
1. Extract the project to a local workstation folder.
2. Install Python 3.11+ if running from source.
3. Install dependencies with `pip install -r requirements.txt`.
4. Launch with `python main.py`.
5. Log in with the seeded admin account and change or manage users as needed.

## Backups
- Database backups are written from inside the app to `Backups/`.
- Keep a copy of the SQLite database file for full recovery.

## Moving to another workstation
Copy:
- the project folder
- the SQLite database file
- the `config/` folder
- the `Backups/` folder if you want prior backup history

## Test data
The app reads local test files from:
- `TestData/Site1`
- `TestData/Site2`

You can later point the app at a different test-data root from Settings.
