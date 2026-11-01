# Sökråd och svenska sökhypoteser

Första profilen är dunörtssvärmare, Proserpinus proserpina. Källbelagda danska
och tyska sökråd skiljs från redaktionella svenska geografiska och ekologiska
hypoteser. Ingen svensk växtförekomst, sannolikhet eller övervintringsförmåga
har modellerats. Hypoteser exporteras aldrig som observationer på kartan.

Data ligger i data/watch/search-guides.json i GeoMap-pilotgrenen och importeras
reproducerbart till search_guides i den separata SQLite-databasen. Varje råd
har region, typ och källreferenser. Profiler utan guide visar uttryckligen att
råden saknas. Exportens befintliga GBIF-punkter och observationer ändras inte.

Svenska namn importeras från den redan cachade Dyntaxa-katalogen till
vernacular_names med källidentitet och källans taxonnamn. Exakta namn används;
tre explicita ändelsevarianter hanteras för Proserpinus och Phyllonorycter.
Ingen generell oskarp namnmatchning eller översättning görs. Tomma svenska namn
visas inte. Namnet i Dyntaxa innebär inte att arten förekommer i Sverige.

Sökguideavsnittet är ett biologiskt ogranskat redaktionellt utkast. Danska
handledningen är från 2017; dess historiska lokaler presenteras inte som ett
fullständigt aktuellt utbredningsläge. Svenska områdesförslag utgår från
närheten till kända danska områden och möjliga liknande miljöer. Värdväxter
och mikroklimat måste kontrolleras före en lokal bedömning.

Validering: guideimporten kräver separata typer för fakta och hypoteser och
befintliga HTTPS-källhänvisningar. Tester kontrollerar att felklassade råd
stoppas och att namn utan exakt koppling lämnas tomma. Alla dynamiska texter
HTML-escapas i förhandsvisningen. Ingen tjänsteomstart behövs.
