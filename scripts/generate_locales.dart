import 'dart:convert';
import 'dart:io';

Future<void> main() async {
  print('Fetching Scryfall bulk data info...');
  final client = HttpClient();
  final req = await client.getUrl(Uri.parse('https://api.scryfall.com/bulk-data'));
  req.headers.set('User-Agent', 'PlaytesterApp/1.0');
  req.headers.set('Accept', 'application/json');
  final res = await req.close();
  final resBody = await res.transform(utf8.decoder).join();
  final bulkData = jsonDecode(resBody);
  
  String? jsonlUri;
  for (final obj in bulkData['data']) {
    if (obj['type'] == 'all_cards') {
      jsonlUri = obj['jsonl_download_uri'] ?? obj['download_uri'];
    }
  }
  
  if (jsonlUri == null) {
    print('Error: Could not find all_cards URI');
    exit(1);
  }
  
  print('Found URI: $jsonlUri');
  print('Downloading and processing stream...');
  
  final downloadReq = await client.getUrl(Uri.parse(jsonlUri));
  downloadReq.headers.set('User-Agent', 'PlaytesterApp/1.0');
  final downloadRes = await downloadReq.close();

  final targetLangs = {'it', 'es', 'fr', 'de', 'pt'};
  final Map<String, Map<String, dynamic>> localizedCards = {}; 
  
  int count = 0;
  
  Stream<String> stringStream;
  if (jsonlUri.endsWith('.gz')) {
    stringStream = downloadRes.transform(GZipCodec().decoder).transform(utf8.decoder).transform(LineSplitter());
  } else {
    stringStream = downloadRes.transform(utf8.decoder).transform(LineSplitter());
  }
  
  await for (final line in stringStream) {
    if (line.trim().isEmpty || line == '[' || line == ']') continue;
    String cleanLine = line;
    if (cleanLine.endsWith(',')) cleanLine = cleanLine.substring(0, cleanLine.length - 1);
    
    try {
      final card = jsonDecode(cleanLine);
      final lang = card['lang'];
      if (targetLangs.contains(lang)) {
        final oracleId = card['oracle_id'];
        if (oracleId == null) continue;
        
        final key = '${oracleId}_$lang';
        
        if (!localizedCards.containsKey(key)) {
          final locData = {
            'oracle_id': oracleId,
            'lang': lang,
            'name': card['printed_name'] ?? card['name'],
            'type_line': card['printed_type_line'] ?? card['type_line'] ?? '',
            'printed_text': card['printed_text'] ?? '',
            'flavor_text': card['flavor_text'] ?? '',
            'image_url': card['image_uris']?['normal'] ?? card['image_uris']?['large'] ?? ''
          };
          if (card['card_faces'] != null && (card['card_faces'] as List).isNotEmpty) {
            locData['name'] = card['card_faces'][0]['printed_name'] ?? card['card_faces'][0]['name'] ?? card['printed_name'] ?? card['name'];
            locData['printed_text'] = card['card_faces'][0]['printed_text'] ?? '';
            locData['image_url'] = card['card_faces'][0]['image_uris']?['normal'] ?? locData['image_url'];
          }
          
          localizedCards[key] = locData;
        }
      }
    } catch (e) {
      // Ignore single parse errors
    }
    
    count++;
    if (count % 10000 == 0) print('Processed $count lines... found ${localizedCards.length} localized cards');
  }
  
  print('Finished parsing. Total localized cards kept: ${localizedCards.length}');
  
  final outFile = File('locales.json');
  final outData = localizedCards.values.toList();
  outFile.writeAsStringSync(jsonEncode(outData));
  
  print('Compressing to locales.json.gz');
  final bytes = outFile.readAsBytesSync();
  final gzBytes = GZipCodec().encode(bytes);
  File('locales.json.gz').writeAsBytesSync(gzBytes);
  
  print('Done! Compressed size: ${gzBytes.length / 1024 / 1024} MB');
}


