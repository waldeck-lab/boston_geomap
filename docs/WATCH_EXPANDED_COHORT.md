# Utökat bevakningsurval och Högintressant

2026-11-01. Standardlistan utökas från 14 till 50 källscreenade förslag.
Theria primaria (vinterfrostmätare i den lokala Dyntaxa-katalogen) ingår.
Detta ersätter tidigare dokumenterade antal, inte tidigare granskningskrav.

Urvalet söker nordisk-baltiska checklistarter som inte har svensk markering,
finns i den europeiska värdväxtmatrisen och har minst ett licensierat publikt
fynd 2015–2026 i DK/DE/PL/LT/LV/EE inom latitud 53–60. Publika svenska positiva
GBIF-träffar spärrar arten tills de bedömts. Även tre nya svenska storfjärilar
ur ET:s årsrapport för 2024 spärras uttryckligen. Sökningar och urvalsbeslut
finns i selection-review.json och source/review/gbif. Ny screening görs med
scripts/review_watch_selection.py --stage ... --count 50.

Detta är en transparent screening, inte ett bevis att alla 50 saknas i Sverige,
är pågående expandörer eller lika sannolika som svenska nyfynd. Taxonomiska
revideringar, historiska fynd och dagsaktuella uppgifter kan kräva ändringar.
Vid otillräckligt underlag får listan bli kortare än det begärda antalet.

## Expansionsbedömning

Separat versionerad originaldata: data/watch/expansion-evidence.json. Import
till expansion_assessments (schemaversion 4). Tabellen skiljer dokumenterad
expansion, nytt landsfynd och ej belagd expansion. Ogranskade arter har null.

Tre arter får inledningsvis Högintressant:
- Proserpinus proserpina: belagd fortsatt spridning i Danmark, underlag 2017.
- Brenthis daphne: nordlig expansion i Baltikum och Centraleuropa, 2016/2022.
- Carcharodus alceae: nordlig/västlig expansion i Belgien, analys 2023.

Period, riktning, motivering, geografisk begränsning, källor och granskningsdag
visas för varje bedömd art. Flaggorna är redaktionella bevakningsprioriteringar,
inga modellresultat eller ankomstsannolikheter. Äldre källperioder skrivs ut;
en 2017-källa presenteras inte som en ny undersökning från 2026.

Theria primaria är relevant men saknar flagga: långvarig dansk förekomst är
belagd; ny expansion är inte belagd i granskade källor. Clepsis dumicolana har
ett nytt danskt landsfynd 2023 men får inte automatiskt expansionsflagga.

Gränssnittet visar stjärna och text i artvalet, ett förklarande avsnitt på
artkortet och ett filter för bara högintressanta arter. Svenska namn används
där de finns. Avsaknad av flagga ska inte läsas som belagd stabil utbredning.

## Fyndkoncentration

Visa fyndkoncentration är förvalt men kan slås av. MapLibre ritar ett heatmap-
lager lokalt i användarens webbläsare. Serverns L4-GPU används inte. Varken
fyndtäthet eller dess färger visar svensk habitatlämplighet eller risk.
Det begränsade urvalet (högst 300 GBIF-poster per art, 2000–2026) har olika
observationsinsats och ska inte användas för automatisk expansionsanalys.

## Validering och återställning

14 tester omfattar befintliga importregler och den nya spärren mot att kalla
ett rent landsfynd för belagd expansion. Förhandsvisningsbygget kontrollerar
också expansionsflaggors underlag. JavaScript syntaxkontrolleras före bygge.
Endast pilotgrenarna och pilotdatabasen ändras. Databaskopia före utökningen:
stage/watch-pilot-20261101/future-species-before-50-20261101.sqlite.
