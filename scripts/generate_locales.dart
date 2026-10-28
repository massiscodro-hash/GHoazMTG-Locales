import 'dart:convert';
import 'dart:io';

Future<String> _fetchBulkUri() async {
  for (int attempt = 1; attempt <= 3; attempt++) {
    final client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 30)
      ..idleTimeout = const Duration(seconds: 60);
    try {
      print('Fetching Scryfall bulk data info (attempt $attempt/3)...');
      final req = await client.getUrl(Uri.parse('https://api.scryfall.com/bulk-data'));
      req.headers.set('User-Agent', 'PlaytesterApp/1.0');
      req.headers.set('Accept', 'application/json');
      final res = await req.close();
      if (res.statusCode != 200) {
        throw HttpException('Bulk data status code: ${res.statusCode}');
      }
      final resBody = await res.transform(utf8.decoder).join();
      final bulkData = jsonDecode(resBody);

      String? jsonlUri;
      for (final obj in bulkData['data']) {
        if (obj['type'] == 'all_cards') {
          jsonlUri = obj['jsonl_download_uri'] ?? obj['download_uri'];
        }
      }
      if (jsonlUri != null && jsonlUri.isNotEmpty) {
        return jsonlUri;
      }
      throw Exception('Could not find all_cards URI in Scryfall response');
    } catch (e) {
      print('Warning on bulk-data metadata attempt $attempt: $e');
      if (attempt == 3) rethrow;
      await Future.delayed(Duration(seconds: 5 * attempt));
    } finally {
      client.close(force: true);
    }
  }
  throw Exception('Unreachable');
}

Future<Map<String, Map<String, dynamic>>> _downloadAndExtract(String jsonlUri) async {
  final targetLangs = {'it', 'es', 'fr', 'de', 'pt'};
  final Map<String, Map<String, dynamic>> localizedCards = {};

  for (int attempt = 1; attempt <= 3; attempt++) {
    final client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 60)
      ..idleTimeout = const Duration(minutes: 5);
    try {
      print('Downloading and processing stream (attempt $attempt/3): $jsonlUri');
      final downloadReq = await client.getUrl(Uri.parse(jsonlUri));
      downloadReq.headers.set('User-Agent', 'PlaytesterApp/1.0');
      final downloadRes = await downloadReq.close();
      if (downloadRes.statusCode != 200) {
        throw HttpException('Download status code: ${downloadRes.statusCode}');
      }

      int count = 0;
      Stream<String> stringStream;
      if (jsonlUri.endsWith('.gz')) {
        stringStream = downloadRes
            .transform(GZipCodec().decoder)
            .transform(const Utf8Decoder(allowMalformed: true))
            .transform(const LineSplitter());
      } else {
        stringStream = downloadRes
            .transform(const Utf8Decoder(allowMalformed: true))
            .transform(const LineSplitter());
      }

      await for (final line in stringStream) {
        if (line.trim().isEmpty || line == '[' || line == ']') continue;
        String cleanLine = line;
        if (cleanLine.endsWith(',')) {
          cleanLine = cleanLine.substring(0, cleanLine.length - 1);
        }

        try {
          final card = jsonDecode(cleanLine);
          final lang = card['lang'];
          if (targetLangs.contains(lang)) {
            final oracleId = card['oracle_id'];
            if (oracleId == null) continue;

            final key = '${oracleId}_$lang';

            if (!localizedCards.containsKey(key)) {
              final locData = <String, dynamic>{
                'oracle_id': oracleId,
                'lang': lang,
                'name': card['printed_name'] ?? card['name'],
                'type_line': card['printed_type_line'] ?? card['type_line'] ?? '',
                'printed_text': card['printed_text'] ?? '',
                'flavor_text': card['flavor_text'] ?? '',
                'image_url': card['image_uris']?['normal'] ??
                    card['image_uris']?['large'] ??
                    '',
              };
              if (card['card_faces'] != null &&
                  (card['card_faces'] as List).isNotEmpty) {
                locData['name'] = card['card_faces'][0]['printed_name'] ??
                    card['card_faces'][0]['name'] ??
                    card['printed_name'] ??
                    card['name'];
                locData['printed_text'] =
                    card['card_faces'][0]['printed_text'] ?? '';
                locData['image_url'] =
                    card['card_faces'][0]['image_uris']?['normal'] ??
                        locData['image_url'];
              }

              localizedCards[key] = locData;
            }
          }
        } catch (_) {
          // Ignore single line parse errors
        }

        count++;
        if (count % 50000 == 0) {
          print(
              'Processed $count lines... found ${localizedCards.length} localized cards');
        }
      }

      if (localizedCards.isEmpty) {
        throw Exception('No localized cards extracted');
      }

      print(
          'Finished parsing on attempt $attempt. Total localized cards kept: ${localizedCards.length}');
      return localizedCards;
    } catch (e) {
      print('Error during stream processing (attempt $attempt/3): $e');
      if (attempt == 3) {
        if (localizedCards.length > 50000) {
          print(
              'Proceeding with ${localizedCards.length} cards already extracted before stream end.');
          return localizedCards;
        }
        rethrow;
      }
      print('Retrying in ${10 * attempt} seconds...');
      await Future.delayed(Duration(seconds: 10 * attempt));
    } finally {
      client.close(force: true);
    }
  }
  return localizedCards;
}

Future<void> main() async {
  final jsonlUri = await _fetchBulkUri();
  final localizedCards = await _downloadAndExtract(jsonlUri);

  final outFile = File('locales.json');
  final outData = localizedCards.values.toList();
  outFile.writeAsStringSync(jsonEncode(outData));

  print('Compressing to locales.json.gz');
  final bytes = outFile.readAsBytesSync();
  final gzBytes = GZipCodec().encode(bytes);
  File('locales.json.gz').writeAsBytesSync(gzBytes);

  print(
      'Done! Compressed size: ${(gzBytes.length / 1024 / 1024).toStringAsFixed(2)} MB');
}
