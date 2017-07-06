SELECT
    `<ticker>` AS symbol,
    DATE_PARSE(`<date>`, "yyyyMMdd") AS tradeDate,
    `<open>` AS open,
    `<high>` AS high,`<low>` AS low,
    `<close>` AS close,
    `<vol>` AS volume
INTO "{{ work.path }}/{{ work.file.base }}.json"
FROM "{{ work.file.path }}"
WITH CSV FORMAT