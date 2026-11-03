# Beslut om norska och förbisedda NRM-arter, 3 november 2026

Tolv arter läggs till i betan. Tre av de femton granskade namnen avvaktar.
Betan får därmed **78 bevakningsarter och 9 svenska referensarter**.

## Tillägg

| Art | Bedömning och viktig begränsning |
|---|---|
| Coleophora algidella | Norsk förekomst stöds av inventeringsrapport från 2022 med namngiven artbekräftare. Inga återpublicerbara GBIF-punkter i valt område. Värdväxtuppgifter och bestämning kräver försiktighet. |
| Stilbia anomala | Långvarig norsk kustförekomst och relevant hedhabitat. Inte belagd ny immigrant eller pågående expansion. |
| Ostrinia quadripunctalis | Norska belägg och dokumenterad slåttermarksmiljö. Värdväxtuppgifterna är motstridiga; anknytning till blodnäva är inte likställt med ett larvuppfödningsbelägg. |
| Capperia britanniodactylus | Norsk expertbedömning redovisar flera sydnorska lokaler. Eftersök styrs av verifierad Teucrium scorodonia. |
| Clepsis mehli | Nordlig regional bevakningsart, med norska belägg och kustängar som sökingång. Larvvärd okänd; ingen sydlig expansionsart. |
| Scrobipalpa costella | Aktuella norska och andra grannlandsbelägg. Besksöta ger en konkret sökingång. NRM:s äldre frånvarouppgift för Norge är inaktuell. |
| Phyllonorycter hostis | Norska belägg och fruktträdsanknytning. Minor och yttre utseende räcker inte alltid för att skilja från närstående arter. |
| Gracillaria loriolella | Norska belägg även i svensk bestämningsnyckel. Askanknytning ger sökriktning. Inga nya expansionstolkningar utifrån databasantal. |
| Coleophora filaginella | Finska/baltiska belägg och dokumenterad värdväxt i öppna sandmiljöer. |
| Coleophora inulae | Åländsk förekomst och ett uttryckligt publicerat eftersöksförslag för Roslagen i Naturvårdsverkets åtgärdsprogram. |
| Coleophora niveistrigella | Baltisk regional kandidat med dokumenterad Gypsophila-anknytning. Litet aktuellt fyndunderlag. |
| Coleophora polonicella | Baltiska belägg och dokumenterad sandvedelanknytning. Svenska sökförslaget gäller visuell spaning och innebär ingen insamling av fridlyst värdväxt. |

Alla tolv har separata avsnitt för utbredning, habitat, värdväxter, fenologi/
eftersök och svenska sökhypoteser. Ingen ny expansionsflagga sätts: regional
relevans är inte samma sak som belagd expansion. Statusen är fortsatt
källscreenad bevakningskandidat, med biologisk expertgranskning kvar.

## Avvaktande namn

- **Coleophora carelica:** svensk museipost MZLU:ENT:265350 kräver kontroll.
  Den gör att arten inte automatiskt kan presenteras som svenskt förstafynd.
- **Coleophora sibiricella:** Skogforsks sammanställning anger osäker svensk
  förekomst. Noll svenska GBIF-poster räcker inte för att avfärda den uppgiften.
- **Scrobipalpa reiprichi:** norsk och alpin barcodevariation väcker frågan om
  kryptiska arter. Taxonomisk granskning behövs före en svensk sökprofil.

## Underlag och kartdata

Kontrollen omfattar aktuella GBIF-sökningar för samtliga femton namn, den
rättade nordisk-baltiska checklistan med senare granskade uppdateringar,
NRM:s individuella artblad, Lepiforum och kompletterande primärkällor.
Ej listad betyder inte bevisad frånvaro, särskilt för ännu opublicerade fynd.

Norska kandidater får ett uttryckligt kartomfång som inkluderar Norge och
Finland till 72° N. De östliga Coleophora-tilläggen använder det befintliga
Finlandsomfånget. Ingen svensk fyndpunkt importeras. De tolv nya historiska
serierna innehåller totalt **435 unika, licensierade och accepterade poster**,
med fullständig API-paginering enligt kontrollen. Algidella har noll kartposter;
dess publicerade norska belägg redovisas som litteratur, utan påhittad koordinat.

Historiska serier behåller avstämningsdatum 2 november 2026 för konsekvens med
befintligt underlag. Artbesluten och de separata förekomstkontrollerna är från
3 november. Alla 75 tidigare tidsfiler är oförändrade enligt SHA-256-jämförelse.

## Bilder

Sju nya visuellt granskade vuxenbilder sparas lokalt med individuell licens,
upphov, originalkälla, metadatahash och SHA-256. Tillsammans med befintligt
material finns nu 82 vuxenbilder för 87 profiler.

Fem uttryckligt granskade bildluckor gäller C. algidella, Clepsis mehli,
C. filaginella, C. inulae och C. polonicella. Säck-/larvbilder eller bilder med
otydligt upphov/licens används inte som vuxenporträtt. En biologiskt relevant
art utesluts inte för att en lämplig bild ännu saknas.

## Spårbarhet och återställning

Beslut per art och källhashar finns i GeoMap:
`data/watch/norway-species-decisions.json`. Källmaterial och API-cache ligger
i betans stage under `source/norway-species-20261103` respektive
`source/norway-nrm-review/api`. Bildsökningar och alternativ ligger i
`stage/image-review/80` till `91`; slutliga foton och bildluckor finns i WCR:s
bildmanifest.

Före import sparades en konsekvent SQLite-kopia, export och tidsfilshashar i
`/proj/boston/beta/wcr/stage/watch-pilot-20261101/before-norway-20261103/`.
Produktionsdatabas och produktionstjänster ingår inte i ändringen.

Relevant verifiering: 29 GeoMap-tester, 8 WCR-tester, 4 tidslinjetester och
renderingskontroll av samtliga 87 faktarutor. Artbilderna har granskats visuellt.

## Några avgörande källor

- [Norsk algidella-inventering, MU2022-68, s. 3 och 19](https://www.nasjonalparkstyre.no/uploads/files_naeroyfjorden/Fagrapportar/MU2022-68-Insektundersokelser-Undredal.pdf)
- [Artsdatabanken: Capperia britanniodactylus](https://lister.artsdatabanken.no/rodlisteforarter/2021/8777)
- [Biofokus: norska slåttermarksinventeringar](https://biofokus.no/insekter-i-slattemark-pa-ostlandet-i-2017-og-2018/)
- [Naturvårdsverkets åtgärdsprogram för krissleinsekter](https://www.naturvardsverket.se/globalassets/media/publikationer-pdf/6600/978-91-620-6632-1.pdf)
- [Skogforsks lärksammanställning, uppdatering 2025](https://www.skogforsk.se/contentassets/291b5a51ad0444ee8a79da7b46f8cfb1/kunskapssammanstallning-lark-2023_uppdatering-2025.pdf)
- [Lund/GBIF: svensk museipost för carelica](https://api.gbif.org/v1/occurrence/1042877446)
- [Lepiforum: Scrobipalpa reiprichi och taxonomifrågan](https://lepiforum.org/wiki/page/Scrobipalpa_reiprichi)
