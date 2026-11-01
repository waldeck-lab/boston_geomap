# Korrigerat urval: bevakningsförslag och svenska referensarter

2026-11-01. Detta ersätter det första tekniska arturvalet i WATCH_PILOT.md.
Den första pilotens urval efter datatillgång var otillräckligt för syftet:
Cameraria ohridella och flera andra arter var redan kända från Sverige.

## Standardlistan

Nu visas 14 bevakningsförslag för första svenska fyndet. Redan svenska arter
ligger separat och visas endast om användaren väljer referensarter. Posten
Macrosaccus robiniella har motstridiga uppgifter och visas inte i standardlistan.
Gamla profiler och fynd raderas inte; klassningarna och beläggen lagras i nya
candidate_assessments (schemaversion 2) i samma fristående pilotdatabas.

Förslagen är källscreenade, inte slutligt expertbedömda. Att en art saknas i
de granskade svenska källorna är inte bevis för frånvaro 2026. Ingen automatisk
slutsats om expansion, övervintring eller kommande etablering dras.

## Granskningsmetod

1. Nordic-Baltic Checklist 2017 används med explicita landkolumner. Parsern
   måste stämma med publikationens 3259 arter totalt och 2804 svenska arter.
   Kastanjemal och andra kända svenska kontrollarter måste ha S-markering.
2. Positiva landtillägg i de vetenskapliga uppdateringarna 2021 och 2025 läggs
   till. Borttagningar och synonymrevisioner kräver separat taxonomigranskning;
   en gammal svensk markering tas därför inte automatiskt bort för att släppa
   igenom en art. Tvåspaltiga PDF:er läses i rå läsordning så landkoder inte
   kopplas till arten i grannspalten.
3. BAB:s öppna originalsammanfattning för 2025 innehåller tolv nya svenska
   småfjärilsarter. Dessa spärras som förstafyndskandidater, inklusive
   Phyllonorycter issikii. De senaste prenumerationsfulltexterna har inte lästs.
4. En separat svensk GBIF-kontroll söker alla publika positiva svenska fynd,
   utan års-, licens- eller koordinatfilter. Alla träffar utesluter arten från
   förstafyndslistan tills fynden bedömts. En introducerad gäst eller ett
   felbestämt fynd måste granskas, inte automatiskt räknas som etablering.
5. Förslag måste ha licensierade, georefererade grannlandsfynd 2015–2026 inom
   vald front, utöver värdväxtunderlag. Fyndantal ger inget mått på expansion.
6. Kompletterande litteratursökning kontrollerar konflikter. Motstridiga svenska
   uppgifter sätter posten åt sidan även vid noll GBIF-träffar. Exempelvis
   Macrosaccus robiniella nämns tillsammans med Sverige i en senare artikels
   indexerade kartbeskrivning, men fulltexten gick inte att kontrollera.

Skriptet scripts/review_watch_selection.py sparar frågor, svar och exakt urval
i source/review och selection-review.json. Den vanliga importen kräver nu detta
granskningsunderlag. Både import och förhandsvisningsbygge stoppar om en svensk,
ogranskad eller motsägelsefullt klassad art förs in som standardkandidat.

## BAB:s uttryckliga bevakningsförslag

Blastobasis phycidella ligger först. I originalrapporten om fyndåret 2022 pekar
Bengt Å. Bengtsson ut arten för eftersök i södra Skåne (ET 144, 2023, sidan 10).
Det är ett publicerat bevakningsförslag, inte ett svenskt fynd. Den fria
original-PDF:en har lästs; inga bilder eller längre textpassager återpubliceras.

Primärkällor:

- https://www.entomologiskforening.no/wp-content/uploads/2025/06/Aarvik_et_al_2017_Nordic-Baltic_Checklist_of_Lepidiptera.pdf
- https://www.entomologiskforening.no/wp-content/uploads/2025/06/Aarvik-et-al-2021.pdf
- https://doi.org/10.61698/nje.v72n1.9
- https://et-online.nu/index.php/contents/article/view/234 (fyndåret 2025, publicerad 2026)
- https://et-online.nu/index.php/contents/article/view/206 (fyndåret 2024, publicerad 2025)
- https://et-online.nu/index.php/contents/article/view/129 (fyndåret 2022, publicerad 2023)

## Körning och kontroller

```sh
cd /proj/boston/beta/pavag-pilot-20261101/geomap
/proj/boston/beta/wcr/runtime/geomap-venv/bin/python scripts/review_watch_selection.py \
  --stage /proj/boston/beta/wcr/stage/watch-pilot-20261101 --count 14
/proj/boston/beta/wcr/runtime/geomap-venv/bin/python scripts/build_watch_pilot.py \
  --stage /proj/boston/beta/wcr/stage/watch-pilot-20261101
/proj/boston/beta/wcr/runtime/geomap-venv/bin/python -m unittest discover \
  -s tests -p 'test_watch*.py'
```

Nya frågor måste hämtas innan första offlinebygget för det nya arturvalet.
Efteråt fungerar --offline som tidigare. Förhandsvisningen byggs med det
befintliga WCR-skriptet build_watch_preview.py och ligger kvar på samma URL.
Hela kandidatlistan och dess belägg finns i selection-review.json. Den äldre
tekniska pilotens dokumenterade antal gäller inte längre standardlistan.

Verifiering: 11 enhetstester, komplett återbyggnad från cachade svar och ett
negativt byggtest där en svensk referensart avsiktligt sätts in som kandidat.
Det felaktiga bygget måste stoppas utan att skapa en webbsida. I webbläsaren
kontrolleras att standardlistan saknar kastanjemal, att BAB-förslaget öppnas
först och att referensgruppen endast visas efter ett aktivt val.

Ordinarie beta, produktionsdatabaser och produktionsdrift ändras inte av detta.
