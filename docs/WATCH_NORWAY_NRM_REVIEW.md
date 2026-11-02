# Norge och NRM: urvalskontroll 2026-11-02

## Resultat och avgränsning

Betans befintliga 66 bevakningsarter och 9 svenska referenser är oförändrade.
Den här genomgången är en kontrollista för fortsatt artgranskning, inte ett
godkännande av alla namn som framtida svenska förstafynd. Fyndantal i GBIF är
databasposter, inte nödvändigtvis unika fynd eller tecken på expansion.

Hela det sparade NRM-grannlandsurvalet om 304 namn har nu kontrollerats,
tillsammans med checklistans norska, ej svenska arter och berörda namn från
parserrättningen. Unionen omfattar 340 namn:

| Resultat | Antal |
|---|---:|
| Redan visade i betan | 42 |
| Svenska enligt senare checklista/årsrapport | 14 |
| Publika svenska fyndposter som behöver granskas | 44 |
| Närliggande fynd 2015–2026, behöver individuell artgranskning | 138 |
| Inga aktuella närliggande GBIF-poster i sökningen | 73 |
| Taxonomisk matchning behöver granskas | 29 |

138 betyder alltså **inte** 138 bekräftade missade kandidater. Även redan
avvaktande arter från Finlandskontrollen återkommer här. GBIF-nollor bevisar
inte frånvaro. Källor, datum, matchat taxon, sök-URL:er och beslutskoder finns i
`data/watch/norway-nrm-audit.json`. Råa API-svar ligger i betans stage-cache
`source/norway-nrm-review/api/`.

## Varför Coleophora föll bort

Både **Coleophora algidella** och **C. carelica** finns i den sparade
NRM-listan och i det lokala värdväxtunderlaget. De saknades alltså inte hos NRM.
I 2017 års nordisk-baltiska PDF läste vår parser mindre tryckta
synonym-/undersläktesrader, Goniodoma och Metriotes, som nya accepterade
släkten. Därför fick flera Coleophora-rader fel kombination med Metriotes.
Kontrollen av totalsummorna 3 259 arter och 2 804 svenska arter avslöjade inte
namnfelet eftersom antalet rader var oförändrat.

Rättningen använder originalets typografi för accepterade släktesrubriker,
med PDF-hash, 1 030 kontrollerade släktesrubriker och regressionstest för just
Coleophora/synonymerna. **256 släktesplaceringar** påverkas jämfört med den
gamla parsningen. Det omfattar svenska arter och alternativa kombinationer;
det är inte 256 nya kandidater och inte ett påstående om dagens giltiga
kombination för varje namn. Äldre Finland-/NRM-kontroller ska därför inte
betraktas som fullständiga inventeringar.

Andra begränsningar bidrog: det första urvalet avbröts efter önskat antal arter,
den geografiska fronten omfattade inte Norge/Finland och värdväxtmatrisen
användes som ett tidigt urvalsfilter. Den tidigare rättade licensavgränsningen
för kartpunkter får heller inte avgöra biologisk förekomst.

Reproduktion:

```
python3 scripts/build_watch_genus_index.py --pdf STAGE/source/review/checklist-2017.pdf --output /tmp/checklist-genera.json
```

Jämför JSON-innehållet med `data/watch/checklist-2017-accepted-genera.json`.
Käll-PDF: https://www.entomologiskforening.no/wp-content/uploads/2025/06/Aarvik_et_al_2017_Nordic-Baltic_Checklist_of_Lepidiptera.pdf

## Coleophora algidella: norsk förekomst bekräftad i publicerad källa

- NRM kallar arten ängsfrylesäckmal och anger norsk förekomst. Checklistan 2017
  anger Island och Norge, inte Sverige.
- Gaarder & Gustad, rapport MU2022-68, redovisar arten från fältarbetet
  30 juni 2022 vid Aurlandsfjorden. Förordet är daterat 26 februari 2023.
  Kai Berggren tackas för artbekräftelsen. Rapporten beskriver en splittrad
  norsk utbredning, bland annat Lærdal, yttre Hordaland och från Bodø norrut.
- Samma rapport uppger att genitalier skiljer arten från C. striatipennella
  medan DNA inte gör det i det diskuterade materialet. Bestämningskontroll är
  därför central. Ett norskt belägg är inte i sig belagd expansion mot Sverige.
- GBIF-kontrollen för accepterat taxon 5120816 gav SE=0, NO=0 och FI=0.
  Den publicerade norska förekomsten visar varför GBIF inte får vara enda
  inträdesvillkoret. Ingen fiktiv koordinat eller påhittad GBIF-post har skapats.
- Arten är en relevant **nordisk bevakningskandidat för fortsatt granskning**.
  Inläggning i artvyn behöver ett uttryckligt stöd för publicerade belägg som
  alternativ till GBIF-antal och ett norskt kartomfång. Den befintliga
  expansionsflaggan ska inte sättas utan separat expansionsbelägg.
- Värdväxtuppgifterna är motstridiga: NRM:s Luzula-uppgift ska inte presenteras
  som säker när Lepiforum diskuterar osäkerheten och belägg på Caryophyllaceae.

Källor:

- http://www3.nrm.se/en/svenska_fjarilar/c/coleophora_algidella.html
- https://www.nasjonalparkstyre.no/uploads/files_naeroyfjorden/Fagrapportar/MU2022-68-Insektundersokelser-Undredal.pdf (s. 3 och avsnitt Stigen–nedre)
- https://lepiforum.org/wiki/page/Coleophora_algidella
- https://www.sef.nu/download/entomologisk_tidskrift/et_1999/1999%2037_42.pdf (svenska C. paeltsaella; ursprungligen misstänkt algidella)

## Coleophora carelica: svensk statuskonflikt

NRM:s äldre sida anger sällsynt förekomst i södra Finland. Men Lunds
museidatabas listar ett svenskt exemplar från Öland. GBIF speglar en svensk
bevarad specimenpost, **MZLU:ENT:265350**, katalognummer 265350, GBIF
1042877446, accepterat taxon 5120664. Den publika GBIF-posten saknar datum,
samlare och bestämmare och hänvisar till museet för mer lokaluppgift.

Detta är en **statuskonflikt som behöver sakkunnig kontroll**, inte tillräckligt
stöd för att slå fast etablering eller ett godkänt svenskt fynd. Arten läggs
därför inte automatiskt in bland förslag på första svenska fynd. NRM-namnet
är östlig röllikasäckmal; dess frånvaro i vårt tidigare urval berodde dels på
parserfelet, dels på att hela NRM-listan ännu inte hade stämts av.

- http://www3.nrm.se/en/svenska_fjarilar/c/coleophora_carelica.html
- https://ento.biomus.lu.se/search.php?country=Sweden&family=Coleophoridae&taxa=Lepidoptera
- https://api.gbif.org/v1/occurrence/1042877446

## Fler namn att ta vidare

Norska checklistnamn utan svenska träffar i denna GBIF-kontroll och med
norska poster 2015–2026 omfattar bland annat nedanstående. Antalen är
kontrollunderlag, inte prioritetspoäng. Habitat, norsk status, svensk status,
eventuell expansion och artbestämning återstår att väga samman.

| Art | Norska GBIF-poster 2015–2026 |
|---|---:|
| Stilbia anomala | 163 |
| Scrobipalpa reiprichi | 47 |
| Ostrinia quadripunctalis | 31 |
| Capperia britanniodactylus | 23 |
| Clepsis mehli | 21 |
| Scrobipalpa costella | 21 |
| Phyllonorycter hostis | 14 |
| Gracillaria loriolella | 6 |

Efter släktesrättningen finns också bland annat C. sibiricella, C. filaginella,
C. inulae, C. niveistrigella och C. polonicella i granskningskön. De är ännu
inte godkända nya arter i betan. Alla 340 granskade namn, även negativa och
osäkra utfall, sparas så att nästa utökning inte börjar med ännu ett begränsat
handplockat urval.

## UI i samma betaändring

Vita kartpaneler är halvtransparenta; vänsterpanelen kan smalnas och fällas
ihop; tidslinjen kan fällas ihop separat och pausar då uppspelningen. År och
filter behålls. Panelval sparas i webbläsaren. På mobil startar tidslinjen
minimerad när inga sparade val finns. Kontrollerna har minst 44 px tryckyta.
Användarens systemval för minskad transparens respekteras.
