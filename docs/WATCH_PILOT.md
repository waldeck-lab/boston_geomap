# Framtida arter / På väg hit — första datapiloten

## Säkrad beta före piloten

Gemensam annoterad tagg i båda repona: `beta-pre-pavag-20261101T195255Z`.
Checkpoint-commits: WCR `1b58ed7`, GeoMap `f0b3e11`.
Privat återställningskopia:
`/proj/boston/beta/wcr/ops/snapshots/beta-pre-pavag-20261101T195255Z/`.
Den innehåller körande kodträd inklusive dist/ospårade filer, ops-konfiguration,
online-backuper av konton, observationslager och dokumentationsdatabas samt
integritetskontrollerat manifest med SHA-256. Konfiguration/konton ska aldrig
läggas i Git eller exponeras via webbservern. Arkiven har privat åtkomst.
Gitstatus i manifestet dokumenterar tidigare utelämnade datafiler i beta;
dessa utelämningar och gamla editorfiler har inte checkats in som pilotändringar.

## Isolering och databas

Separata arbetskopior under `/proj/boston/beta/pavag-pilot-20261101/{wcr,geomap}`,
båda på `pilot/pavag-hit-20261101`. Den vanliga betans kod och services ligger
kvar på checkpointen. Produktion är inte ändrad. Endast pilotens statiska,
skrivskyddade förhandsvisning läggs till under betans befintliga dist-katalog:
https://beta.waldeck.se/watch-pilot/index.html

Egen SQLite-databas:
`/proj/boston/beta/wcr/stage/watch-pilot-20261101/future-species.sqlite`.
Schema/migration v1 skapas idempotent av `geomap/watch_store.py`. Inga skrivningar
sker i kontodatabasen eller det befintliga shared.sqlite.

Tabeller: `schema_versions`, `sources`, `import_runs`, `candidates`,
`host_associations`, `habitat_requirements`, `phenology`,
`ecological_requirements`, `occurrences`, `occurrence_records`, `source_traits`.
Värdväxter och biotoper kan ha explicit primärmarkering men okänt värde bevaras
som NULL. Region, originalfält, källa och granskningsstatus följer fakta.
Livsstadier har separata månadsbelägg; flera månader kan korsa årsskiftet.
Importerad brittisk fenologi är inte ett svenskt sökfönster.

## Arturval och begränsningar

15 arter används för att testa källtäckning. 13 är explorativa poster,
Vanessa cardui och Saturnia pavonia är referenser för dataövningen. Detta är
inte en granskad kandidatlista över nya svenska arter. Även flera explorativa
arter kan redan förekomma eller vara etablerade i Sverige. Svensk status och
expansionsbedömning står uttryckligen som unknown/unreviewed för alla poster.
Ingen frånvaro i Sverige eller etableringssannolikhet har härletts.

Pilotnamnen finns i `scripts/build_watch_pilot.py`. Källmaterialets exakta
namnmatchningar återanvänds; ingen osäker fuzzy-match görs. Dyntaxa-koppling
används när namnet matchar den sparade taxonomin exakt. Omatchat betyder en
granskningslucka, inte att ett taxon saknas i Dyntaxa eller Sverige.

Fyndprovet använder anonyma GBIF-anrop med länderna DK/DE/PL/LT/LV/EE,
latitud 53–60, eventdatum 2000-01-01 till 2026-11-01, koordinater utan angivna
geospatiala fel, positiv förekomst och CC0/CC BY 4.0. Högst 300 resultat per
art; detta är inte fullständiga historiker, expansionsanalys eller statistiskt
representativa stickprov. Noll resultat betyder bara noll i detta urval.
Ospecificerat livsstadium/koordinatosäkerhet förblir okänt. Poster med
informationWithheld/dataGeneralizations eller ogiltiga data utesluts.
Ursprungs-ID, GBIF-nyckel, datasetnyckel, licens och kvalitetsflaggor bevaras.
Identiska ursprungs-URL:er delar en kanonisk post men behåller källposter.

## Verifierade källor och verkliga fynd i profileringen

Gaytán, European Lepidoptera–Plant Associations:
https://doi.org/10.1002/ece3.73004
Den publicerade datalänken ledde från konceptpost 16914288 till versionspost
https://doi.org/10.5281/zenodo.18197194 — den exakta posten och checksummorna
sparas i source/zenodo-record.json. CC BY 4.0.

Den nedladdade matrisen är semikolonseparerad, har 5 152 rader och 3 275
växtkolumner. En källcell är tom och ett pilotnamn återkommer med avslutande
blanksteg. Tomma celler räknas som saknade, inte 0, och normaliserade identiska
namn får unionen av dokumenterade relationer. 0 betyder ingen registrerad
relation, inte biologiskt utesluten värdväxt. Primär värdväxt kan inte härledas
ur denna binära matris. Källans README innehåller en ofullständig DOI-placeholder;
den ersätts inte med en gissning, utan den riktiga versionsposten citeras.

Cook m.fl., brittiska/irländska traits 2024:
https://doi.org/10.5285/dbc7cc17-cbbd-49dd-bab4-8e8855768d66
Open Government Licence enligt EIDC:s katalog. CSV i Windows-1252 med två
rubrikrader, inte en vanlig UTF-8 DictReader-tabell. Kodlistan i de medföljande
supportdokumenten kontrollerades: common_hostplants=1 anger vanligast i
källregionen; habitat=1 anger använt reproduktionshabitat, inte primär rang.
Originalvärden bevaras även när tolkningen kräver fortsatt granskning.
Denna källa täcker fem pilotarter med publicerad fenologi och tre med
habitatdata. Databasen skiljer dessa uppgifter från daterade fynd och från
framtida modelluppskattningar. Övriga egenskaper, exempelvis övervintring och
generationer, bevaras med originalkoder och källregion.

GBIF: https://techdocs.gbif.org/en/openapi/v1/occurrence
Varje fråga, tidpunkt och JSON-svar sparas i source/gbif/. Licens kontrolleras
även på varje importerad post. Denna pilot använder inga personliga API-token,
bilder eller privata fynd. Större vetenskapliga uttag bör senare göras som
versionsbara GBIF-downloads med DOI, inte utökas till oändlig sök-API-paginering.

## Återskapa och granska

```sh
cd /proj/boston/beta/pavag-pilot-20261101/geomap
/proj/boston/beta/wcr/runtime/geomap-venv/bin/python scripts/build_watch_pilot.py \
  --stage /proj/boston/beta/wcr/stage/watch-pilot-20261101 --offline
/proj/boston/beta/wcr/runtime/geomap-venv/bin/python -m unittest discover \
  -s tests -p test_watch_store.py
cd ../wcr
python3 scripts/build_watch_preview.py \
  --data /proj/boston/beta/wcr/stage/watch-pilot-20261101/pilot-export.json \
  --output /proj/boston/beta/wcr/wcr/frontend/dist/watch-pilot
```

Källarkivet innehåller matris, Zenodo-metadata, traits-ZIP, supporting docs,
taxonomisnapshot och cachade GBIF-svar. Inventeringen med luckor och exakta
sökfilter finns i inventory.json. Saknad cache stoppar offlinekörning.
Omkörning dubblerar inte relationer eller fynd och skriver inte över status/
granskningsanteckningar. Ändrad checksumma under samma källversion stoppar
importen. Ny källversion ska införas uttryckligen, inte skriva över källhistorik.

Förhandsvisningen är en fristående dataprototyp som använder den redan
installerade MapLibre-versionen. Ingen ny webbtjänst, auth-rutt eller GPU-process
startas. Den återanvänder inte ännu alla React-komponenter eller SV/EN-stöd;
det görs vid riktig integration. Endast det uttryckligen märkta pilotunderlaget
är publikt läsbart. En vanlig Vite-build rensar dist och tar bort förhandsvisningen;
kör byggskriptet igen när den behövs. Betans normala GeoMap fungerar oförändrat.

## Nästa biologiska granskningssteg

Välj ut verkliga kandidater tillsammans med användaren; belägg svensk status
och expansionsfront i checklistor/faunistiska källor. Fyll primär värdväxt,
biotop och regional fenologi för dessa arter, särskilt småfjärilarna. Därefter
kan svenska värdväxtfynd importeras med motsvarande proveniens och kombineras
med habitatunderlag. Inga svenska växtfynd, habitatmasker, sannolikhetsmodeller,
betalningsfunktioner eller GPU-beräkningar har skapats i denna första pilot.

För återgång: pilotarbetskopiorna kan lämnas åt sidan, då betans kod redan står
på checkpointen. Förhandsvisningen kan tas bort genom en vanlig betabuild.
Vid en senare verklig återställning används samordnade taggar och privat
manifest; skriv inte över ny användaraktivitet med en gammal kontosnapshot utan
att först bedöma vad som då förloras. Inga backuper eller konfigurationer pushas.
