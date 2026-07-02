# Passivering af ungesager

Robot der håndterer passivering af ungesager i KMD Nexus ved ophør, herunder fjernelse af medarbejder, forløb og organisationstilknytninger baseret på bestillinger fra sagsbehandlere.

## Hvad gør robotten?

1. Henter aktive opgaver med titlen 'Luk sag - Tyra' fra aktivitetslisten 'Opgaver til Tyra' i KMD Nexus, oprettet inden for de seneste 7 dage, og tilføjer dem til arbejdskøen.
2. For hvert element i arbejdskøen hentes borger, skema og opgave fra Nexus via de tilknyttede referencer.
3. Afgør om sagen er en kompensationssag ('Sag: Støtte til børn og unge med funktionsnedsættelse') eller en socialsag.
4. **Kompensationssager:** Kontrollerer om der er aktive indsatser på forløbet. Er der aktive indsatser, oprettes en blokerende opgave til den ansvarlige sagsbehandler, og behandlingen udsættes. Er der ingen aktive indsatser, fjernes medarbejderen fra forløbet, forløbet lukkes, og borgeren fjernes fra de relevante organisationer.
5. **Socialsager:** Gennemgår alle aktive forløb. For forløb med aktive indsatser oprettes en blokerende opgave og forløbet rapporteres. For forløb uden aktive indsatser fjernes sagsbehandleren fra forløbet, forløbet lukkes, og borgeren fjernes fra de relevante ungerådgivningsorganisationer.
6. Lykkedes alle passiveringsopgaver, lukkes Nexus-opgaven og borgeren registreres som behandlet. Ved delvise fejl udskydes opgavens frist med 1 uge og borgeren tilføjes til rapporten for manuel behandling.

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø)
- Adgang til **KMD Nexus** (produktion og database)
- Adgang til **Odense SQL Server**

## Installation

```sh
uv sync
```

## Konfiguration

Credentials registreres i Automation Server:
- `KMD Nexus - produktion`
- `KMD Nexus - database`
- `Odense SQL Server`

| Miljøvariabel | Beskrivelse |
|---|---|
| `ATS_WORKQUEUE_OVERRIDE` | Valgfri tilsidesættelse af arbejdskø-ID |

## Kørsel

```sh
uv run python main.py --queue   # Fyld arbejdskøen
uv run python main.py           # Behandl arbejdskøen
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Klient til Automation Server — arbejdskøer og credentials |
| `odk-tools` | Sporing af behandlede og delvise opgaver samt generering af rapporter |
| `kmd-nexus-client` | Henter borgere, skemaer, opgaver, forløb, organisationer og indsatser fra KMD Nexus |
| `nexus-database-client` | Direkte databaseadgang til KMD Nexus til supplerende forespørgsler |
| `openpyxl` | Håndtering af Excel-filer til rapportgenerering |
| `ruff` | Python-linter og -formatter |

## GDPR og sikkerhed

Robotten behandler CPR-numre på borgere, hvis ungesager lukkes, samt navne og organisationstilknytninger på de ansvarlige sagsbehandlere. Oplysningerne hentes fra KMD Nexus og behandles udelukkende i hukommelsen under kørslen. Rapporter med borgerdata gemmes i Automation Server og bør kun være tilgængelige for autoriserede medarbejdere.
