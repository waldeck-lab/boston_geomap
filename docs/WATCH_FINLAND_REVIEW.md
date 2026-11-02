# Finland: upptäckta luckor i På väg hit

Uppföljningen är genomförd: tio nya artkort har publicerats efter genomgång av
hela 72-artskön. Se WATCH_FINLAND_DECISIONS.md för aktuella artvisa beslut.
Texten nedan beskriver den ursprungliga inventeringen före detta tillskott.

Granskat 2 november 2026. Ingen art eller observationspunkt har lagts till automatiskt.

## Orsak

Den nordisk-baltiska checklistan innehöll Finland, men pilotens fyndfront var
Danmark, Tyskland, Polen, Litauen, Lettland och Estland, med latitud 53–60.
Finland ingick alltså inte i fyndsökningen, och en enkel komplettering av landskoden
skulle fortfarande missa merparten av Finland genom latitudtaket.
Urvalet begränsades dessutom till en bestämd pilotstorlek och till arter i
värdväxtmatrisen. Nola estonica saknas helt i den matrisen. Detta är ytterligare
en urvalsbias: avsaknad av ekologisk information får inte tolkas som biologisk
irrelevans. En nybeskriven art behöver också hittas via senare nationella källor.

## Genomförd kontroll

157 arter med finsk men utan svensk landskod i checklistan 2017, inklusive
landstilläggen 2021 och 2025, samt Nola estonica och Athetis correpta från finska
källor. Totalt 159 arter kontrollerades mot GBIF med exakt matchat taxonnamn.
Svensk kontroll gäller alla positiva GBIF-poster; finsk aktualitet gäller 2015–2026.
Biologiskt urval begränsas inte av bild- eller kartlicenser.

- 84 arter hade aktuella finska poster och inga svenska GBIF-poster.
- 12 av dessa finns redan i piloten; **72 återstår i granskningskön**.
- 24 hade svenska publika poster som behöver bedömas.
- 39 saknade aktuella finska GBIF-poster.
- 12 kräver taxonomisk kontroll. Inga anrop misslyckades.

Detta är inte en fullständig aktuell Finland–Sverige-differens. Den äldre
checklistans svenska kolumn och GBIF kan missa senare svenska fynd. Kön innehåller
också nordliga och östliga specialister, tillfälliga gäster och namn som måste
granskas; inga av dem får automatiskt kallas expanderande eller högintressanta.

## Första arter att granska närmare

| Art | Finska GBIF-poster 2015–2026 | Svenska GBIF-poster i kontrollen |
|---|---:|---:|
| Nola estonica | 83 | 0 |
| Xylomoia strix | 11 | 0 |
| Eversmannia exornata | 18 | 0 |
| Athetis correpta | 10 | 0 |

Poster är databasposter, inte nödvändigtvis unika observationer. Noll svenska
poster är inte bevis för svensk frånvaro. Nola estonica ska hållas åtskild från
Nola aerugula; nyupptäckt/nybeskriven är inte samma sak som nyinvandrad.

Nola finns i FinBIF:s Artlistan 2022 (publicerad 2023) och i Nocturnas rapport
för 2024. Rapportens tabell 2 gäller arter nya för **övervakningen**, inte första
fynd i Finland. Där står Nola under 2023 och Athetis correpta under 2024.
Den ursprungliga beskrivningen från 2021 belägger Nolas artstatus och påpekar
att äldre samlingar behöver undersökas innan artens historia bedöms.

## Fortsättning som behövs före publicering av nya arter

Kontrollera senare svenska årsrapporter, artbestämning och finska originalfynd.
Skilj dokumenterad expansion från äldre förekomst och förändrad bestämning.
Inför en finsk/östlig fyndfront med eget deklarerat geografiskt omfång och
datering. Låt saknade värdväxtuppgifter vara en synlig kunskapslucka i stället
för ett uteslutningskrav. Komplettera först därefter artkort, licenskontrollerade
kartpunkter, historik och bildval. De 65 befintliga artbilderna påverkas inte.

## Källor och reproduktion

- [FinBIF Artlistan 2022](https://cdn.laji.fi/files/checklists/2022/Lajiluettelo2022_Checklist2022.pdf)
- [Nocturna 2024, Baptria 4/2025](https://www.ymparisto.fi/sites/default/files/documents/Baptria%204_2025%20Nocturna%20vuosiraportti%202024.pdf)
- [Nola estonica: originalbeskrivning 2021](https://doi.org/10.11646/zootaxa.5082.5.1)
- [Nordisk-baltisk uppdatering 2025](https://www.entomologiskforening.no/wp-content/uploads/2025/08/nje-vol72-no1-94-107.pdf)

Fullständig granskningskö, fråge-URL:er, namnmatchningar och antal finns i
finland-omissions-audit.json. Serverns stage/source/finland-review innehåller
cachade API-svar med hämtningstid samt de lästa finska rapporterna.
Verktyget scripts/audit_watch_finland.py gör om kontrollen från dessa källor.
