default:
    @just --list --unsorted

# Installerer nødvendige pakker for andre kommandoer
bootstrap:
    @printf "Sørger for at nødvendige pakker er installert\n"
    @uv --version > /dev/null 2>&1 || brew install uv
    @nais --version > /dev/null 2>&1 || brew install nais || brew tap nais/tap

# Setter opp miljøet
install: bootstrap
    @printf "Setter opp miljøet\n"
    @uv sync --dev

# Oppgrader dependencies og setter opp miljøet på nytt
update-dependencies: bootstrap
    @printf "Oppgraderer dependencies\n"
    @uv sync --upgrade --dev

# Kjører linting
lint:
    @printf "Linter kode\n"
    @uv run ruff check .

# Formaterer koden
format:
    @printf "Formaterer kode\n"
    @uv run ruff format .
    @uv run sqlfluff fix sql --rules CV03 --dialect ansi  

# Generer requirements.txt
requirements:
    @printf "Genererer requirements.txt\n"
    @uv run sync --no-dev
    @uv pip freeze > requirements.txt
    @uv run sync --dev
    @grep -v '^-e ' requirements.txt > requirements.txt.temp && mv requirements.txt.temp requirements.txt
