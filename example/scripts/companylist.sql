INSERT INTO "{{ work.path }}/{{ work.file.base }}.json" WITH JSON FORMAT (Symbol, Name, Sector, Industry)
SELECT Symbol, Name, Sector, Industry, `Summary Quote`
FROM "{{ work.file.path }}"
WITH CSV FORMAT