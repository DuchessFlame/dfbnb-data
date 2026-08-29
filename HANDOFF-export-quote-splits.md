# The exporter was splitting rows in half — found, fixed, data repaired

Found while building the Player Icons page: the S10–S12 scoreboard icons had no
texture filename. The cause was not the icons. It was the export scripts, and it
had been quietly damaging data across several record types since around April.

---

## What was happening

Every `!!!Wordpress - Export*ToTSV.pas` packs a record into one delimited string,
stores it, then unpacks it again:

```pascal
parts.Delimiter := FS;  parts.StrictDelimiter := True;
parts.DelimitedText := rec;
```

`TStringList.DelimitedText` applies **quote** processing. `StrictDelimiter` only
stops whitespace acting as a delimiter — it does **not** turn off `QuoteChar`,
which defaults to `"`. So any value that *starts* with a double quote was
re-parsed as a quoted token: the opening quote eaten, the closing quote ending
the token, and the remainder becoming an extra list entry. The line was then
rebuilt by fixed index (`parts[2] + TAB + parts[3] …`), so every column after
that field shifted one place right and the tail fell off the end.

Same record, two exports:

```
March : NNAM = '"The Chase" Poster'
July  : NNAM = 'The Chase'   XALG_Flags = ' Poster'   ...and everything after +1
```

The exposure is exactly "a quoted word at the start of a display name" —
`"Megasloth" Pelt Rug`, `"Improved" Slot Machine`, `"Excavator Suit" - Final
Steps`. ENTM took the brunt because the Atom Shop names hundreds of player
titles and icons that way.

---

## The damage

Newest export of each family:

| Export | Rows split |
|---|---|
| ENTM | **219** live, 242 per PTS pull |
| BOOK | 3 |
| ACTI | 3 |
| COBJ | 1 |
| MISC | 1 |
| NOTE | 1 |
| everything else | none, at any version |

Every other family is immune — no version of them has a single value starting
with a quote, so there was nothing for the bug to bite. CHAL, ALCH, PLYT, EMOT,
CNDF, LVLI, WEAP and NPC were all checked and cleared.

---

## What changed

**1. The root cause — 31 `.pas` scripts.** `QuoteChar := #0` added at all 96
`DelimitedText` sites. Fresh exports are clean from here on. Backups sit
alongside as `*.bak-20260828-2115`.

**2. The committed data — repaired in place.** 64 export files across `tsv/` and
`tsv/pts/`, touching only the broken lines. Every other line is byte-identical,
including its line ending, so the diff is exactly the rows that were wrong and
nothing else. Verified: no characters were lost or gained in any repaired row —
merging removes one tab and appends one empty field, so line length is conserved
by construction.

**3. `src/tsv_repair.py`** — the shared repair. Finds the misaligned rows by the
modal position of a landmark value, works out which boundaries are spurious, and
re-joins them. It refuses to touch a row unless the evidence includes a
space-led fragment, the one shape nothing but a split produces. Earlier drafts
without that veto "repaired" ten CHAL/NPC/WEAP rows that were never broken.

**4. `tools/repair_export_quote_splits.py`** — rewrites the files. `--dry-run` to
report, `--check` for CI. Idempotent.

**5. Both workflows** run the repair before any builder, so an export taken with
an older copy of the scripts still can't reach a build.

### Why the data was repaired rather than every reader

Seventeen builders read ENTM alone, each with its own `csv.DictReader` call.
Threading a fix through all of them is a large diff with no way to be sure none
was missed, and it wouldn't help `tools/`, a notebook, or the next script. Fixing
the data once fixes every consumer. The exports aren't sacred — the build
already regenerates `CURV_Export_*_POINTS.tsv` and rewrites `season_rewards.tsv`.

---

## Proof it is right

`ENTM_Export_March_2026.tsv` predates the regression, so it is ground truth.

- Of the 219 repaired rows, 192 also exist in March and **191 reproduce it
  exactly** on FULL/DESC/NNAM.
- The one difference is a real Bethesda rename — "N.C.R. Trooper" → "NCR
  Trooper" — confirmed by two sibling records the repair never touched.
- Across the whole file, 5,936 of 5,961 shared records agree. The 25 that differ
  are ordinary drift (DESC rewrites, `T45`→`T-45`, `Beckly`→`Beckley`) and none
  is a row the repair touched.

`python src/tsv_repair.py --verify` re-runs that and fails loudly if the drift
ever jumps.

---

## What it fixed on the site

- **Player Icons / scoreboards** — the S10–S12 icons had the texture *path* where
  the filename should be, so they had been falling back to season art.
- **Atom Shop** — two truncated descriptions restored. "How hard could it be?"
  was the whole stored description; the rest of the sentence was in the next
  column.
- **Buff Stations** — the Radiation Glove Box description was cut off mid-sentence.
- **COBJ recipes** — the "Glow of the Ghoul Movie Projector" recipe was **missing
  entirely**, because its name was split in two and never resolved. It's back.

### One thing to check

The Radiation Glove Box image URL changed, because its real `ETDI` finally reads
correctly:

```
was: /guide-images/camp-items/buff-stations/atx_radiationglovebox.avif
now: /guide-images/camp-items/buff-stations/score_s10_camp_utility_radiation_glove_box.avif
```

The new name is the correct one, but the file may not be on the server under it.
The source texture is in your Lookbook dump at
`Armour Data Dump/textures/atx/storefront/camp/utility/score_s10_camp_utility_radiation_glove_box*.dds`
— convert and upload, or rename the existing AVIF. It's one image.

---

## Committing

`tsv/guide_index.tsv` and `tsv/season_rewards.tsv` in the working tree are from
the Player Icons page, not this repair. Worth splitting into two commits if you
want the data-repair diff to stand on its own.
