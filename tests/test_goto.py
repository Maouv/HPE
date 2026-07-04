"""
HPE Playwright test — connect + goto + evaluate.
Run after extension is connected and a tab is open.
"""
import asyncio
import sys
from playwright.async_api import async_playwright

GATEWAY = "http://localhost:8765"

async def main():
    async with async_playwright() as p:
        print("[1] Connecting to gateway...")
        browser = await p.chromium.connect_over_cdp(GATEWAY, timeout=45000)
        print(f"[2] Connected. Contexts: {len(browser.contexts)}")
        pages = []
        for ctx in browser.contexts:
            pages.extend(ctx.pages)
        print(f"[3] Pages: {len(pages)}")
        if not pages:
            print("[!] No pages. Open https://example.com in Mises first.")
            await browser.close()
            sys.exit(1)

        page = pages[0]
        print(f"[4] Using page: {page.url}")

        # Test goto
        print("[5] Navigating to https://example.com...")
        try:
            resp = await page.goto("https://example.com", timeout=30000)
            print(f"[6] Goto OK: status={resp.status if resp else 'None'}")
        except Exception as e:
            print(f"[6] Goto FAILED: {e}")

        # Test evaluate
        print("[7] Evaluating document.title...")
        try:
            title = await page.evaluate("document.title")
            print(f"[8] Title: {title}")
        except Exception as e:
            print(f"[8] Evaluate FAILED: {e}")

        # Test set_content
        print("[9] set_content test...")
        try:
            await page.set_content("<h1>HPE Works</h1>", timeout=15000)
            h1 = await page.evaluate("document.querySelector('h1')?.textContent")
            print(f"[10] set_content OK: h1={h1}")
        except Exception as e:
            print(f"[10] set_content FAILED: {e}")

        await browser.close()
        print("[11] Done.")

asyncio.run(main())
