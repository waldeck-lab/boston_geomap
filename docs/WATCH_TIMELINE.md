# Historisk fynduppspelning i betapiloten

Levererat 2 november 2026. Föräldraversioner före funktionen:
WCR b8dec2d och GeoMap 44d3157 på pilotgrenen.

## Användning

Välj art och tryck Till start för samtliga godkända fynd före 2000 i
tidsunderlaget. Spela går sedan årsvis från 2000 till 2026, 900 ms per steg.
Pausa eller dra årsreglaget för att granska ett år. Uppspelningen stannar
vid sista året. Spela vid slutet börjar om från startläget.

Ackumulera fynd är förvalt: gamla punkter ligger kvar till och med valt år.
Avmarkering visar enbart det valda årets fynd. Före-2000-steget är alltid
ett samlat historiskt startläge. Årsstaplarna visar däremot antal fynd per
tidssteg, inte en ackumulerad summa. Den lodräta markören följer valt år.

Månad och stadium filtrerar både graf och karta. Okänd månad får ingen
påhittad månadsplacering. Artbyte och dolt dokument stoppar spelaren.
Artbyte avbryter pågående datahämtning och gamla svar kan inte skriva över
den nya artens tidsserie. Arter utan godkända kartfynd har avstängd spelare.
Kartan behåller samma geografiska utsnitt under en uppspelning.

Ungefärliga litteraturmarkörer filtreras också på tid, men ingår inte i
fyndhistogrammet eller värmekartans intensitet.

## Underlag och avgränsning

65 tidsfiler täcker pilotens 56 bevakningsarter och 9 svenska referenser.
Hämtningen gav 19 832 GBIF-poster och 19 658 godkända unika fynd. 174 poster
uteslöts vid datum- och kvalitetskontrollen. Inga identitetskonflikter hittades.
Alla söksidor i de valda frågorna hämtades. 4 417 godkända fynd är äldre än
2000. 609 poster har bevarad grövre datumprecision än dag.

För dunörtssvärmare finns 108 godkända fynd, varav ett före 2000.
Det är inte ett påstående om alla historiska fynd av arten i Europa.

Frågorna gäller Danmark, Tyskland, Polen, Litauen, Lettland och Estland,
latitud 53–60° N, förekomst PRESENT, koordinater utan flaggat geografiskt
problem och licens CC0 eller CC BY 4.0. Daterade fynd från år 1 till och med
2 november 2026 kan ingå. Dagar efter slutdatum, oklara årintervall,
generaliserade/undanhållna uppgifter och ogiltiga koordinater utesluts.
År- och månadsdaterade observationer bevaras utan fabricerade datumdelar.

Grafen visar registrerade fynd, inte inventeringskorrigerad täthet,
populationsstorlek eller en verifierad expansionstakt. År 2026 är ett delår.
Sen digitalisering av äldre fynd och varierande rapportering påverkar bilden.

## Lagring och reproduktion

Tidsdata ligger separat från pilotens begränsade 2000–2026-urval och
ändrar inte SQLite-databasens kanoniska observationer eller produktion.
Filerna finns under pilotens stage/timeline och hämtas av webbläsaren först
när arten väljs. Om hämtningen misslyckas visas ett meddelande och det äldre
begränsade piloturvalet, utan att uppspelningen aktiveras.

Originalsvaren cachas med fråge-URL, hämttid och SHA-256. Varje exporterad
observation behåller ursprungsidentitet, GBIF-nyckel, dataset, licens och
datumprecision. Identiteter används för att undvika dubletter.
Datasetlänkar finns under Om tidsunderlaget, och klickbara fynd har GBIF-länk.

Byggordning i befintliga pilotworktrees:

1. Bygg pilot-export.json med scripts/build_watch_pilot.py.
2. Kör GeoMaps scripts/build_watch_timeline.py med --stage och
   --as-of 2026-11-02. --offline kan återanvända färdig cache.
3. Kör WCR:s scripts/build_watch_preview.py med --data och --output som förut.
   Publicering kräver manifest och tidsfiler för alla exporterade arter.

GBIF-frågorna pagineras i sidor om 300. Stora sökningar delas vid år 2000
och därefter per år för att undvika långsamma djupa sidpositioner.
HTTP 429/5xx ger begränsade återförsök. Sökningar över API-gränsen kräver
GBIF:s download API i stället för tyst trunkering.

[GBIF:s API-dokumentation](https://techdocs.gbif.org/en/openapi/v1/occurrence)

## Validering

21 Python-tester och 4 JavaScript-tester passerar. De kontrollerar bland
annat historiska och partiella datum, framtida/otydliga datum, licens- och
kvalitetsvillkor, startgränsen 1999/2000, ackumulerat/enskilt år samt samma
månads- och stadieurval i graf och karta. Alla 65 faktaprofiler renderas,
JavaScript syntaxkontrolleras och tidsfilernas paginering är fullständig.

Webbläsarkontroll: dunörtssvärmarens startläge 1/108, årsmarkörens rörelse,
paus, 2026 som enskilt år 3/108, junifilter 37/108 och säker paus vid artbyte
till Lithophane leautieri utan återpublicerbara kartpunkter.
