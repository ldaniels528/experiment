INSERT INTO "{{ work.path }}/{{ work.file.base }}.json" (Symbol, Name, Sector, Industry)
WITH JSON FORMAT
SELECT Symbol, Name, Sector, Industry, `Summary Quote`
FROM "{{ work.file.path }}"
WITH CSV FORMAT