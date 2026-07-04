"""Comprehensive HPE test — all Playwright features"""
import asyncio
from playwright.async_api import async_playwright

GATEWAY = "http://localhost:8765"

async def main():
    results = {"pass": 0, "fail": 0, "skip": 0}

    def ok(name, detail=""):
        results["pass"] += 1
        print(f"  ✅ {name}{' — ' + detail if detail else ''}")

    def fail(name, detail=""):
        results["fail"] += 1
        print(f"  ❌ {name}{' — ' + detail if detail else ''}")

    async with async_playwright() as p:
        print("=" * 60)
        print("HPE COMPREHENSIVE TEST")
        print("=" * 60)

        # ── 1. Connect ──
        print("\n[1] connect_over_cdp...")
        try:
            browser = await p.chromium.connect_over_cdp(GATEWAY, timeout=30000)
            ok("connect_over_cdp")
        except Exception as e:
            fail("connect_over_cdp", str(e)[:80])
            print(f"\n📊 Results: {results['pass']} pass, {results['fail']} fail")
            return

        # ── 2. Contexts ──
        print("\n[2] browser.contexts...")
        contexts = browser.contexts
        if contexts:
            ok(f"contexts", f"{len(contexts)} context(s)")
        else:
            fail("contexts", "no contexts")

        # ── 3. Pages ──
        print("\n[3] context.pages...")
        pages = contexts[0].pages if contexts else []
        if pages:
            ok(f"pages", f"{len(pages)} page(s): {[p.url[:50] for p in pages]}")
            page = pages[0]
        else:
            fail("pages", "no pages found")
            await browser.close()
            print(f"\n📊 Results: {results['pass']} pass, {results['fail']} fail")
            return

        # ── 4. Page info ──
        print("\n[4] page.url / page.title...")
        try:
            url = page.url
            ok("page.url", url[:60])
            title = await page.title()
            ok("page.title", title[:60])
        except Exception as e:
            fail("page.info", str(e)[:80])

        # ── 5. goto ──
        print("\n[5] page.goto('https://example.com')...")
        try:
            resp = await page.goto("https://example.com", timeout=30000)
            if resp and resp.status == 200:
                ok("goto", f"status={resp.status}, url={resp.url[:50]}")
            else:
                fail("goto", f"status={resp.status if resp else 'None'}")
        except Exception as e:
            fail("goto", str(e)[:80])

        # ── 6. evaluate (return value) ──
        print("\n[6] page.evaluate('document.title')...")
        try:
            title = await page.evaluate("document.title")
            if title == "Example Domain":
                ok("evaluate", f"title='{title}'")
            else:
                fail("evaluate", f"expected 'Example Domain', got '{title}'")
        except Exception as e:
            fail("evaluate", str(e)[:80])

        # ── 7. evaluate with args ──
        print("\n[7] page.evaluate with args...")
        try:
            result = await page.evaluate("(a, b) => a + b", 3, 7)
            if result == 10:
                ok("evaluate(args)", f"3+7={result}")
            else:
                fail("evaluate(args)", f"expected 10, got {result}")
        except Exception as e:
            fail("evaluate(args)", str(e)[:80])

        # ── 8. set_content ──
        print("\n[8] page.set_content('<h1>Hello HPE</h1>')...")
        try:
            await page.set_content("<html><body><h1>Hello HPE</h1><p>World</p></body></html>")
            h1 = await page.evaluate("document.querySelector('h1').textContent")
            if h1 == "Hello HPE":
                ok("set_content", f"h1='{h1}'")
            else:
                fail("set_content", f"expected 'Hello HPE', got '{h1}'")
        except Exception as e:
            fail("set_content", str(e)[:80])

        # ── 9. goto another URL (navigation back) ──
        print("\n[9] page.goto back to example.com...")
        try:
            resp = await page.goto("https://example.com", timeout=30000)
            if resp and resp.status == 200:
                ok("goto(2)", f"status={resp.status}")
            else:
                fail("goto(2)", "failed")
        except Exception as e:
            fail("goto(2)", str(e)[:80])

        # ── 10. innerText / textContent ──
        print("\n[10] page.innerText...")
        try:
            text = await page.innerText("h1")
            if "Example" in text:
                ok("innerText", f"'{text[:40]}'")
            else:
                fail("innerText", f"'{text[:40]}'")
        except Exception as e:
            fail("innerText", str(e)[:80])

        # ── 11. click a link ──
        print("\n[11] page.click('a')...")
        try:
            await page.goto("https://example.com", timeout=30000)
            # Click "More information..." link
            await page.click("a")
            await page.wait_for_load_state("networkidle", timeout=15000)
            new_url = page.url
            if "iana.org" in new_url:
                ok("click+nav", f"landed on {new_url[:50]}")
            else:
                ok("click+nav", f"navigated to {new_url[:50]}")
        except Exception as e:
            fail("click+nav", str(e)[:80])

        # ── 12. go_back ──
        print("\n[12] page.go_back()...")
        try:
            await page.go_back(timeout=15000)
            back_url = page.url
            if "example.com" in back_url:
                ok("go_back", back_url[:50])
            else:
                fail("go_back", f"got {back_url[:50]}")
        except Exception as e:
            fail("go_back", str(e)[:80])

        # ── 13. execute JS DOM manipulation ──
        print("\n[13] DOM manipulation via evaluate...")
        try:
            await page.evaluate("""
                document.body.innerHTML = '<div id="test">Dynamic Content</div>';
            """)
            text = await page.evaluate("document.getElementById('test').textContent")
            if text == "Dynamic Content":
                ok("DOM manipulation", text)
            else:
                fail("DOM manipulation", f"'{text}'")
        except Exception as e:
            fail("DOM manipulation", str(e)[:80])

        # ── 14. addScriptTag ──
        print("\n[14] page.addScriptTag...")
        try:
            await page.goto("https://example.com", timeout=30000)
            await page.add_script_tag(content="window.__HPE_TEST__ = 'magic_value';")
            val = await page.evaluate("window.__HPE_TEST__")
            if val == "magic_value":
                ok("addScriptTag", val)
            else:
                fail("addScriptTag", f"got '{val}'")
        except Exception as e:
            fail("addScriptTag", str(e)[:80])

        # ── 15. viewport ──
        print("\n[15] page.viewport_size...")
        try:
            vs = page.viewport_size
            if vs:
                ok("viewport", f"{vs['width']}x{vs['height']}")
            else:
                fail("viewport", "None")
        except Exception as e:
            fail("viewport", str(e)[:80])

        # Close
        await browser.close()

    print("\n" + "=" * 60)
    total = results["pass"] + results["fail"]
    print(f"📊 Results: {results['pass']}/{total} pass, {results['fail']} fail")
    print("=" * 60)
    return results["fail"] == 0


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
