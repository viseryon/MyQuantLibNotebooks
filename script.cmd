cd /d "%~dp0"
if exist "%~dp0main.py" (
    cmd /c uv run main.py
)
exit