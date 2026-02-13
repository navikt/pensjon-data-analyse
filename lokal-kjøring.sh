#!/bin/bash
# chmod +x lokal-kjøring.sh

# Last inn brukernavn og host fra .env (kjører export av DB_USER og DB_HOST)
set -a
source "$(dirname "$0")/.env"
set +a

if [[ -z "$DB_USER" || -z "$DB_HOST" ]]; then
    echo "Error: DB_USER og DB_HOST miljøvariabler ikke satt"
fi

# DBT_DB_TARGET velger target i profiles.yml
echo "Velg DBT_DB_TARGET:"
select db_target in "pen_q2" "pen_prod_lesekopi" "pen_prod"; do
    if [[ -n "$db_target" ]]; then
        export DB_TARGET="$db_target"
        echo "DB_TARGET set to $DB_TARGET"
        break
    else
        echo "Ugyldig valg. Velg 1, 2, eller 3."
    fi
done

# password med skjult input
printf "Enter db-password: "
stty -echo
read DB_ENV_SECRET_PASS
stty echo
printf "\n"
export DB_ENV_SECRET_PASS

# Printer ut bruker
printf "Miljøvariabler satt:\n"
printf "DB_ENV_USER:    %s\n" "$DB_ENV_USER"
