#!/usr/bin/env python3
"""
build_about_json.py
===================
Builds dist/about.json for the /about/ pages on buffsnbrew.com and
theduchessflame.com (rendered by df-bnb-about.js in the dfbnb-child theme).

Inputs:
  src/about/vault_legends.tsv  — one donor per row (column: name)

  Future TSVs can be added here for the other /about/ pages
  (community_groups, contact, follow, support_my_work). Each gets
  picked up by an `if path.exists()` branch below — keep that style
  so the script never crashes when a TSV is missing.

Output:
  dist/about.json  — keyed by page slug (hyphens, matching the URL):
    {
      "vault-legends":     { "title": ..., "subtitle": ..., "donors": [...] },
      "community-groups":  { "title": ..., "subtitle": ..., ... },
      "work-with-me":      { ... },   # was "contact" until Aug 2026
      "follow":            { ... },
      "support-my-work":   { ... }
    }

Usage:
  python src/build_about_json.py
  python src/build_about_json.py --outdir dist
"""

import argparse
import csv
import json
import sys
from pathlib import Path


VAULT_LEGENDS_SUBTITLE = (
    "Thank you to the following players. Without your support and donations, "
    "this website would not have been possible."
)

PLACEHOLDER_SUBTITLE = "Coming soon."

# Follow — intro card copy only. The link cards themselves live in
# _FOLLOW_LINKS in df-bnb-about.js (they carry inline SVG icons, so they
# can't round-trip through JSON cleanly).
#
# This MUST be a real entry rather than a placeholder: renderFollow() falls
# back to its own hardcoded blurb only when pageData.subtitle is falsy, and
# "Coming soon." is truthy — so leaving Follow in the placeholder loop below
# silently shipped "Coming soon." as the live subtitle. (Fixed Aug 2026.)
FOLLOW = {
    "title": "About - Follow",
    "subtitle": (
        "Hey, I’m Kat — also known as DuchessFlame! I create Fallout 76 "
        "guides, break down game updates, and built Buffs n Brew to help "
        "the community get the most out of the Wasteland. Follow me on your "
        "favourite platform to stay in the loop with new guides, patch "
        "breakdowns, and everything in between."
    ),
}

# Support My Work — narrative + action cards. Edit this constant to update
# the page content; the workflow regenerates dist/about.json on push.
SUPPORT_MY_WORK = {
    "title": "About - Support My Work",
    "subtitle": "Hey there, I’m Kat — also known as Duchess!",
    "intro": [
        "I’m an Australian data miner and content creator who makes "
        "Fallout 76 guides — farming routes, event reward breakdowns, "
        "food and chem buff math, season pass tracking, plan checklists, "
        "and everything in between.",
        "I firmly believe knowledge should be accessible to everyone, so the "
        "guides will always be free and ad-free. No paywalls, no popups.",
        "That said, running a site like this isn’t free. Web hosting, "
        "the build pipeline that needs to be ran after every patch and update, image "
        "and video storage, photo and video editing software, and the hours "
        "of comparing game files at 2am all add up. If the guides have saved "
        "you grind time, helped you finish a season pass, or just made the "
        "wasteland a little less confusing, here are a few ways you can help "
        "keep them going:",
    ],
    "actions": [
        {
            "label": "Share My Guides",
            "description": "The single most valuable thing you can do. Drop "
                           "a guide link in your friend group, mention me "
                           "when someone asks “where do I find X” "
                           "on Reddit, share a checklist with your team "
                           "before a public event. Word of mouth is what "
                           "keeps fan sites alive.",
        },
        {
            "label": "Follow Me on Social Media",
            "description": "Following on Facebook, Twitter, Bluesky and "
                           "Discord helps each post reach more vault "
                           "dwellers (the algorithms reward engagement) and "
                           "means you’ll see new guides and patch "
                           "breakdowns the moment they go live.",
            "url": "/about/follow/",  # internal — JS prepends current brand (/df or /bnb)
        },
        {
            "label": "One-Time Donation",
            "description": "Buy me a Ko-fi (coffee). Caffeine is what gets "
                           "me through patch days when Bethesda drops a "
                           "4 GB update at 1am my time and the whole "
                           "pipeline needs to be re-run from scratch.",
            "url": "https://ko-fi.com/duchessflame",
        },
        {
            "label": "Monthly Contribution",
            "description": "Become a regular supporter on Ko-fi. Recurring "
                           "support lets me plan ahead — hosting "
                           "upgrades, better tools, and keeping things "
                           "steady through the quiet months between big "
                           "Fallout updates.",
            "url": "https://ko-fi.com/duchessflame",
        },
    ],
    "closing": [
        "Every bit of support — a kind word in my DMs, a share, your "
        "time, or a donation — helps keep this thing going. Honestly, "
        "knowing the guides are useful to someone is the biggest part of "
        "what keeps me at it.",
        "Thanks for being here :)",
        "— Kat",
    ],
}

# Work With Me — collaborations, sponsorships and creator partnerships.
# Lives at the /about/contact/ slug (URL kept stable so existing links,
# nav entries and redirects keep working); only the display title changed.
# Same shape as SUPPORT_MY_WORK so it reuses renderProsePage in
# df-bnb-about.js — intro paragraphs, action cards, closing lines.
WORK_WITH_ME = {
    "title": "About - Work With Me",
    "subtitle": "Collaborations, sponsorships and creator partnerships.",
    "intro": [
        "I’m passionate about partnering with brands and creators to bring "
        "high-quality, engaging content to gaming communities. With "
        "experience in sponsored posts, collaboration content and product "
        "showcases, I love connecting audiences with products and insights "
        "that genuinely improve their time in the wasteland.",
        "I specialise in a range of content formats — full game guides, "
        "event walkthroughs, infographics, data-mined breakdowns and "
        "product highlights — making it easy for followers to find useful "
        "tips and discover new products. Whether it’s a social post, a "
        "livestream, or a custom written guide, the goal is always "
        "authentic, useful content that actually lands with viewers.",
        "My main focus is Bethesda Game Studios titles — Fallout 76 and "
        "Starfield — but I’m open to writing guides and creating content "
        "for other games too. I also do merch unboxings, mail openings, "
        "and attend gaming events and launches within Australia.",
        "Fellow content creators and streamers: this page is for you as "
        "much as it is for brands. If you want to co-write a guide, "
        "co-host a stream or community event, cross-promote a project, "
        "swap data-mining notes and research, or just get a group together "
        "for a raid or event run — reach out. A lot of the best things on "
        "this site started as a conversation with another creator.",
        "Brands, agencies and creators can get in touch through any of "
        "these:",
    ],
    "actions": [
        {
            "label": "Email",
            "description": "The best option for brands, agencies and "
                           "anything with a media kit, rates or deadlines "
                           "attached. Opens a new email with the subject "
                           "line pre-filled.",
            "url": "mailto:DuchessFlame3215@gmail.com?subject=Collab%20Opportunity",
        },
        {
            "label": "Twitter / X",
            "description": "DMs are open. Good for a quick pitch, a "
                           "creator collab idea, or a first hello before "
                           "moving to email.",
            "url": "https://x.com/duchessflame",
        },
        {
            "label": "Facebook",
            "description": "Message the page directly. Handy if you’ve "
                           "found me through one of the Fallout 76 "
                           "community groups.",
            "url": "https://www.facebook.com/DuchessFlame/",
        },
        {
            "label": "Discord",
            "description": "Best place for creator-to-creator chat — "
                           "guide collabs, event runs, research swaps and "
                           "anything that’s easier as a conversation than "
                           "an email thread.",
            "url": "https://discord.com/invite/wKXRuJ6rBr",
        },
    ],
    "closing": [
        "If you’re not sure whether your idea is a fit, ask anyway — I’d "
        "rather hear it and say “not this time” than have you not bother.",
        "Looking forward to the chance to create something exceptional "
        "together!",
        "— Kat",
    ],
}

# Privacy Policy — written guide (flat prose). Rendered by renderWrittenGuide
# in df-bnb-about.js using the .ab-guide component (style guide → Written
# Guides). Each section is { "title": <H2 or "" for the lead-in>, "html": ... }.
# Wording is Duchess's own (23 Oct 2026) — edit here, not in the JS. When the
# policy changes, update the "Last updated" date in the subtitle too.
PRIVACY_POLICY = {
    "title": "About - Privacy Policy",
    "subtitle": "Last updated: 23 October 2026",
    "sections": [
        {
            "title": "",
            "html": (
                "<p>This policy explains what information theduchessflame.com, "
                "the Buffs n Brew community tools and the DuchessFlame Discord "
                "bots (Brewmaster and Mia Matthews Flame) collect, how we use it, "
                "and the choices you have. In this policy, “we”, “us” and "
                "“DuchessFlame” mean the person who runs this website and its "
                "bots.</p>"
            ),
        },
        {
            "title": "Information we collect",
            "html": (
                "<h3>When you use the website</h3>"
                "<ul>"
                "<li><strong>Account details:</strong> if you register, we keep "
                "your username, email address, display name and any profile "
                "details you choose to add, such as pronouns.</li>"
                "<li><strong>Messages you send us:</strong> messages sent through "
                "the Contact tab, bug reports and guide suggestions, including "
                "anything you attach.</li>"
                "<li><strong>Your checklist and tracker progress:</strong> what "
                "you’ve ticked off on the website’s checklists and trackers. Some "
                "of this is saved only in your own browser.</li>"
                "<li><strong>Technical information:</strong> like most websites, "
                "our web host keeps basic server logs (such as IP address, "
                "browser type and the pages you visit) for security and to keep "
                "the site running. WordPress sets cookies to keep you logged "
                "in.</li>"
                "</ul>"
                "<h3>When you use our Discord bots</h3>"
                "<ul>"
                "<li>Our bots see messages posted in the channels they have "
                "access to. They respond to messages that mention them, reply to "
                "them or use their commands.</li>"
                "<li>They may store your Discord user ID, username and display "
                "name, along with information you give them, such as your time "
                "zone, bug reports, suggestions or to-do requests.</li>"
                "<li>To write replies, the bots send the relevant message text to "
                "an AI service (see “Who we share information with”).</li>"
                "</ul>"
            ),
        },
        {
            "title": "How we use your information",
            "html": (
                "<ul>"
                "<li>To run the website, your account and the community "
                "tools.</li>"
                "<li>To answer your messages, bug reports and suggestions.</li>"
                "<li>To let the Discord bots answer questions, run commands and "
                "help with community moderation. This includes alerting "
                "moderators to suspicious messages, such as attempts to make the "
                "bot open links.</li>"
                "<li>To keep the website and community safe and to fix "
                "problems.</li>"
                "</ul>"
                "<p>We do not sell your information, and we do not use it for "
                "advertising.</p>"
            ),
        },
        {
            "title": "Google user data",
            "html": (
                "<p>Mia Matthews Flame is a personal assistant app used only by "
                "the owner of DuchessFlame, with her own Google accounts. It does "
                "not request access to anyone else’s Google account.</p>"
                "<p>With the owner’s permission, the app accesses:</p>"
                "<ul>"
                "<li><strong>Gmail (read, label/archive and send):</strong> to "
                "show emails from real people in one inbox, to send replies she "
                "writes herself, and to find bills and invoices so they can be "
                "added to her bills list and calendar.</li>"
                "<li><strong>Google Calendar (read and add events):</strong> to "
                "show upcoming events and add reminders and bill due dates she "
                "asks for.</li>"
                "</ul>"
                "<h3>How this data is handled</h3>"
                "<ul>"
                "<li>Access tokens are stored only on the owner’s own "
                "computers.</li>"
                "<li>Email content is read when needed and is not kept "
                "permanently. The exception is bill details (biller, amount, due "
                "date), which are saved to her private bills list.</li>"
                "<li>Email text may be sent to Anthropic’s Claude API so bill "
                "details can be pulled out or a reply drafted. Anthropic’s API "
                "terms state that it does not use this data to train its "
                "models.</li>"
                "<li>Google user data is never sold, never used for advertising, "
                "and never shared with anyone else except as needed to provide "
                "these features, to comply with the law, or for security.</li>"
                "<li>The owner can remove the app’s access at any time at "
                "<a href=\"https://myaccount.google.com/permissions\" "
                "target=\"_blank\" rel=\"noopener\">myaccount.google.com/permissions</a>.</li>"
                "</ul>"
                "<p>The use and transfer of information received from Google APIs "
                "by Mia Matthews Flame will adhere to the "
                "<a href=\"https://developers.google.com/terms/api-services-user-data-policy\" "
                "target=\"_blank\" rel=\"noopener\">Google API Services User Data "
                "Policy</a>, including the Limited Use requirements.</p>"
            ),
        },
        {
            "title": "Who we share information with",
            "html": (
                "<p>We only share information with the services that run the "
                "website and bots:</p>"
                "<ul>"
                "<li><strong>Our web host</strong>, which stores the website and "
                "its data.</li>"
                "<li><strong>Discord</strong>, where the bots operate (see "
                "Discord’s own privacy policy).</li>"
                "<li><strong>Anthropic</strong>, whose Claude AI writes the bots’ "
                "replies and processes the text sent to it.</li>"
                "<li><strong>Google</strong>, for the Gmail and Calendar features "
                "described above.</li>"
                "<li><strong>Buffer</strong>, which we use to schedule our own "
                "social media posts. No member data is sent to Buffer.</li>"
                "</ul>"
                "<p>We may also share information if the law requires it, or to "
                "protect the safety of our members or the community.</p>"
            ),
        },
        {
            "title": "How long we keep it",
            "html": (
                "<ul>"
                "<li>Account details are kept while your account is open.</li>"
                "<li>Messages and reports are kept while they’re useful to the "
                "community. For example, bug reports are kept until the bug is "
                "fixed.</li>"
                "<li>Bot conversation history is short and is cleared "
                "regularly.</li>"
                "<li>You can ask us to delete your information at any time.</li>"
                "</ul>"
            ),
        },
        {
            "title": "Keeping it safe",
            "html": (
                "<p>We use secure (HTTPS) connections, keep passwords and access "
                "keys private, and limit who can see members’ information. No "
                "system is perfectly secure, but we take reasonable steps to "
                "protect your information.</p>"
            ),
        },
        {
            "title": "Your choices",
            "html": (
                "<ul>"
                "<li>You can update or delete your account details on the "
                "website, or ask us to do it for you.</li>"
                "<li>You can ask what information we hold about you, ask us to "
                "correct it, or ask us to delete it.</li>"
                "<li>You can stop the bots seeing your messages by not posting in "
                "the channels they use, or by leaving the server.</li>"
                "</ul>"
            ),
        },
        {
            "title": "Children",
            "html": (
                "<p>Fallout 76 is rated for mature audiences, and Discord requires "
                "users to be at least 13. This website and its community are not "
                "aimed at children, and we do not knowingly collect information "
                "from anyone under 13.</p>"
            ),
        },
        {
            "title": "Changes to this policy",
            "html": (
                "<p>If we change this policy, we’ll update the date at the top of "
                "this page.</p>"
            ),
        },
        {
            "title": "Contact",
            "html": (
                "<p>For questions or privacy requests, email "
                "<a href=\"mailto:duchessflame3215@gmail.com?subject=Privacy%20request\">"
                "duchessflame3215@gmail.com</a> or use the Contact tab on this "
                "website.</p>"
            ),
        },
    ],
}


def read_tsv_column(path: Path, column: str) -> list[str]:
    """Read a single column out of a TSV, stripping blanks. Returns [] if the
    file doesn't exist — pages without a TSV yet get an empty list."""
    if not path.exists():
        return []
    out: list[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            v = (row.get(column) or "").strip()
            if v:
                out.append(v)
    return out


def build(src_dir: Path) -> dict:
    about = {}

    # Vault Legends — donor names
    donors = read_tsv_column(src_dir / "vault_legends.tsv", "name")
    about["vault-legends"] = {
        "title": "About - Vault Legends",
        "subtitle": VAULT_LEGENDS_SUBTITLE,
        "donors": donors,
    }

    # Support My Work — narrative + action cards.
    # Content is static prose so it lives here in the build script rather
    # than a TSV (TSVs are for tabular/growing data like donor lists).
    about["support-my-work"] = SUPPORT_MY_WORK

    # Work With Me — collaborations / sponsorships / creator partnerships.
    # Renamed from "Contact" (slug was /about/contact/) in Aug 2026; a 301
    # in functions.php keeps the old URL working.
    about["work-with-me"] = WORK_WITH_ME

    # Follow — intro card copy. Link cards are rendered from the JS module.
    about["follow"] = FOLLOW

    # Privacy Policy — written guide (sections of H2 + HTML prose).
    about["privacy-policy"] = PRIVACY_POLICY

    # Other /about/ pages — placeholder until content is added.
    # Each is keyed by URL slug; the JS module dispatches by slug.
    for slug, title in [
        ("community-groups", "About - Community Groups"),
    ]:
        about[slug] = {
            "title": title,
            "subtitle": PLACEHOLDER_SUBTITLE,
            "items": [],
        }

    return about


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="src/about", help="Directory containing about TSVs")
    ap.add_argument("--outdir", default="dist", help="Output directory")
    args = ap.parse_args()

    src_dir = Path(args.src)
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "about.json"

    data = build(src_dir)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote {out_path}")
    print(f"  vault-legends donors: {len(data['vault-legends']['donors'])}")
    print(f"  support-my-work    actions: {len(data['support-my-work']['actions'])}")
    print(f"  work-with-me       actions: {len(data['work-with-me']['actions'])}")
    print(f"  privacy-policy     sections: {len(data['privacy-policy']['sections'])}")
    print(f"  follow             subtitle={data['follow']['subtitle'][:48]!r}...")
    for k in ("community-groups",):
        print(f"  {k:<18} title={data[k]['title']!r}, subtitle={data[k]['subtitle']!r}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
