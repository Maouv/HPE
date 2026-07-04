#!/usr/bin/env python3
"""
HPE POC Test — End-to-end Playwright test

Validasi: Playwright connect_over_cdp → new_page → goto → screenshot

Cara jalanin:
  1. Start gateway:    python -m gateway.server
  2. Connect extension di HP (Mises/Kiwi)
  3. Run test:         python tests/test_playwright.py

Kalau screenshot tersimpan dan valid → POC LOLUS.
"""

import asyncio
import sys
import os

from playwright.async_api import async_playwright


GATEWAY_URL = os.environ.get('HPE_GATEWAY', 'http://192.168.1.100:8765')


async def test_basic_connection():
    """Test 1: Connect ke gateway."""
    print(f'\n{"="*60}')
    print(f'Test 1: Basic Connection')
    print(f'Gateway: {GATEWAY_URL}')
    print(f'{"="*60}')

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(GATEWAY_URL)
            print(f'✅ Connected to browser')
            print(f'   Contexts: {len(browser.contexts)}')
            return browser
        except Exception as e:
            print(f'❌ Connection failed: {e}')
            return None


async def test_get_targets(browser):
    """Test 2: List existing tabs."""
    print(f'\n{"="*60}')
    print(f'Test 2: Get Targets (existing tabs)')
    print(f'{"="*60}')

    try:
        if browser.contexts:
            for i, ctx in enumerate(browser.contexts):
                pages = ctx.pages
                print(f'  Context {i}: {len(pages)} pages')
                for j, page in enumerate(pages):
                    print(f'    Page {j}: {page.url}')
        else:
            print('  No contexts found')
        print(f'✅ Target listing OK')
        return True
    except Exception as e:
        print(f'❌ Get targets failed: {e}')
        return False


async def test_new_page(browser):
    """Test 3: Create new page (tab)."""
    print(f'\n{"="*60}')
    print(f'Test 3: New Page (create tab)')
    print(f'{"="*60}')

    try:
        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await ctx.new_page()
        print(f'✅ New page created')
        return page
    except Exception as e:
        print(f'❌ New page failed: {e}')
        return None


async def test_navigate(page):
    """Test 4: Navigate to URL."""
    print(f'\n{"="*60}')
    print(f'Test 4: Navigate')
    print(f'{"="*60}')

    url = 'https://example.com'
    try:
        response = await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        print(f'✅ Navigated to {url}')
        if response:
            print(f'   Status: {response.status}')
        title = await page.title()
        print(f'   Title: {title}')
        return True
    except Exception as e:
        print(f'❌ Navigate failed: {e}')
        return False


async def test_screenshot(page):
    """Test 5: Screenshot — THE critical test."""
    print(f'\n{"="*60}')
    print(f'Test 5: Screenshot')
    print(f'{"="*60}')

    output = '/workspace/hpe/poc_screenshot.png'
    try:
        await page.screenshot(path=output, full_page=False)
        size = os.path.getsize(output)
        print(f'✅ Screenshot saved: {output}')
        print(f'   Size: {size} bytes ({size/1024:.1f} KB)')

        if size < 1000:
            print(f'⚠️  Screenshot suspiciously small — might be blank')
            return False

        print(f'✅ Screenshot looks valid')
        return True
    except Exception as e:
        print(f'❌ Screenshot failed: {e}')
        return False


async def test_evaluate(page):
    """Test 6: JavaScript evaluation."""
    print(f'\n{"="*60}')
    print(f'Test 6: JavaScript Evaluate')
    print(f'{"="*60}')

    try:
        result = await page.evaluate('() => document.title')
        print(f'✅ Evaluate result: "{result}"')

        result2 = await page.evaluate('() => navigator.userAgent')
        print(f'   User-Agent: {result2}')
        return True
    except Exception as e:
        print(f'❌ Evaluate failed: {e}')
        return False


async def test_click(page):
    """Test 7: Click element."""
    print(f'\n{"="*60}')
    print(f'Test 7: Click')
    print(f'{"="*60}')

    try:
        # example.com has a link to "More information..."
        link = page.locator('a').first
        href = await link.get_attribute('href')
        print(f'   Found link: href={href}')

        await link.click(timeout=10000)
        await page.wait_for_load_state('domcontentloaded', timeout=15000)
        print(f'✅ Clicked link, navigated to: {page.url}')
        return True
    except Exception as e:
        print(f'⚠️  Click test skipped or failed: {e}')
        return False


async def test_full_page_screenshot(page):
    """Test 8: Full page screenshot."""
    print(f'\n{"="*60}')
    print(f'Test 8: Full Page Screenshot')
    print(f'{"="*60}')

    output = '/workspace/hpe/poc_screenshot_full.png'
    try:
        await page.goto('https://example.com', wait_until='domcontentloaded', timeout=30000)
        await page.screenshot(path=output, full_page=True)
        size = os.path.getsize(output)
        print(f'✅ Full page screenshot saved: {output}')
        print(f'   Size: {size} bytes ({size/1024:.1f} KB)')
        return True
    except Exception as e:
        print(f'⚠️  Full page screenshot failed: {e}')
        return False


async def main():
    print('\n' + '='*60)
    print('  HPE — Hermes Playwright Extension — POC Test')
    print('='*60)

    results = {}

    # Test 1: Connect
    browser = await test_basic_connection()
    results['connect'] = browser is not None
    if not browser:
        print('\n❌ Cannot continue — connection failed')
        print('\nMake sure:')
        print('  1. Gateway running: python -m gateway.server')
        print('  2. Extension connected in Mises/Kiwi')
        print(f'  3. Gateway URL correct: {GATEWAY_URL}')
        sys.exit(1)

    # Test 2: Get targets
    results['targets'] = await test_get_targets(browser)

    # Test 3: New page
    page = await test_new_page(browser)
    results['new_page'] = page is not None
    if not page:
        print('\n❌ Cannot continue — new_page failed')
        sys.exit(1)

    # Test 4: Navigate
    results['navigate'] = await test_navigate(page)

    # Test 5: Screenshot
    results['screenshot'] = await test_screenshot(page)

    # Test 6: Evaluate
    results['evaluate'] = await test_evaluate(page)

    # Test 7: Click
    results['click'] = await test_click(page)

    # Test 8: Full page screenshot
    results['full_screenshot'] = await test_full_page_screenshot(page)

    # Summary
    print(f'\n{"="*60}')
    print(f'  POC RESULTS')
    print(f'{"="*60}')
    for name, passed in results.items():
        status = '✅ PASS' if passed else '❌ FAIL'
        print(f'  {name:20s} {status}')

    critical = ['connect', 'new_page', 'navigate', 'screenshot']
    all_critical_pass = all(results.get(k, False) for k in critical)

    print(f'\n{"="*60}')
    if all_critical_pass:
        print(f'  🎉 POC LOLUS — Critical tests passed!')
        print(f'  Next: Full implementation (Fase 2)')
    else:
        print(f'  ❌ POC GAGAL — Critical tests failed')
        print(f'  Check logs above for details')
    print(f'{"="*60}\n')

    # Cleanup
    try:
        await page.close()
    except Exception:
        pass
    try:
        browser.close()
    except Exception:
        pass


if __name__ == '__main__':
    asyncio.run(main())
