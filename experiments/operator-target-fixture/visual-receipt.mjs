import assert from 'node:assert/strict';

const APP = 'https://app.example.org';
const VIEWER = 'https://collector.example.org/coinglass/operator';
const name = 'התחברות ל־CoinGlass והמשך איסוף';
const BLUE = [19, 77, 145];
const GREEN = [25, 117, 83];

async function launch(page) {
  const launchButton = page.getByRole('button', {name, exact: true});
  await launchButton.waitFor({state:'visible', timeout:15000});
  assert.equal(await launchButton.count(), 1, 'one BTC pilot button is required');
  const [popup] = await Promise.all([
    page.waitForEvent('popup', {timeout: 15000}),
    launchButton.click({timeout: 15000}),
  ]);
  return popup;
}

async function colorVisible(popup, color) {
  await popup.waitForFunction(expected => {
    const canvas = document.querySelector('#screen canvas');
    if (!canvas || canvas.width < 100 || canvas.height < 100) return false;
    const ctx = canvas.getContext('2d');
    if (!ctx) return false;
    // Only sample three pixels from this credential-free synthetic fixture.
    return [.35, .50, .65].every(x => {
      const pixel = ctx.getImageData(Math.floor(canvas.width*x), Math.floor(canvas.height*.65), 1, 1).data;
      return expected.every((value, i) => Math.abs(pixel[i] - value) <= 10);
    });
  }, color, {timeout: 20000});
}

async function paintThroughViewer(popup) {
  await popup.waitForURL(VIEWER, {timeout: 20000});
  await colorVisible(popup, BLUE);
  const screen = popup.locator('#screen canvas');
  const box = await screen.boundingBox();
  assert.ok(box && box.width > 100 && box.height > 100);
  await screen.click({position:{x: Math.floor(box.width*.50), y: Math.floor(box.height*.65)}});
  await colorVisible(popup, GREEN);
}

async function waitForReopen(page) {
  const end = Date.now() + 100000;
  while (Date.now() < end) {
    await page.reload({waitUntil: 'domcontentloaded', timeout: 15000});
    const button = page.getByRole('button', {name, exact: true});
    try {
      await button.waitFor({state:'visible', timeout:3000});
      if (await button.isEnabled()) return;
    } catch (error) {
      if (error.name !== 'TimeoutError') throw error;
    }
    await new Promise(resolve => setTimeout(resolve, 1500));
  }
  throw Error('fixture workspace did not regain the operator lease');
}

/** Call from the local fixture app's authenticated Playwright test, on the BTC worksheet. */
export async function verifyHostedFixture(page) {
  const sheet = page.url();
  assert.equal(new URL(sheet).origin, APP, 'fixture app origin required');
  const popup = await launch(page);
  let sibling;
  try {
    await popup.waitForURL(VIEWER, {timeout:20000});
    await colorVisible(popup, BLUE);
    // Navigate a second ordinary viewer in the same signed-in context. Browser
    // cookies stay opaque. The server logs an authenticated competing WS denial.
    const competingViewer = await page.context().newPage();
    try {
      await competingViewer.goto(VIEWER, {waitUntil:'domcontentloaded'});
      await competingViewer.locator('#status').filter({hasText:'חלון החיבור נסגר'}).waitFor({timeout:15000});
      assert.equal(await competingViewer.locator('#verify').isHidden(), true);
    } finally {
      await competingViewer.close();
    }
    await paintThroughViewer(popup); // original owner must still control the actual page
    sibling = await page.context().newPage();
    await sibling.goto(sheet, {waitUntil:'domcontentloaded'});
    const competing = sibling.getByRole('button', {name, exact:true});
    await competing.waitFor({state:'visible', timeout:10000});
    assert.equal(await competing.isDisabled(), true, 'an attached viewer must own the lease');
  } finally {
    if (sibling) await sibling.close();
    await popup.close();
  }

  await waitForReopen(page);
  // Abandon after the server allocated a ticket but before the viewer attached.
  // Block this one synthetic admission without reading its request body.
  const admission = 'https://collector.example.org/coinglass/operator/admit';
  let issued;
  const ticketIssued = new Promise(resolve => { issued = resolve; });
  const abandon = async route => { await route.abort(); issued(); };
  await page.context().route(admission, abandon);
  let abandoned;
  let timeout;
  try {
    abandoned = await launch(page);
    await Promise.race([ticketIssued, new Promise((_, reject) => {
      timeout = setTimeout(() => reject(Error('ticket was not issued')), 15000);
    })]);
  } finally {
    clearTimeout(timeout);
    await page.context().unroute(admission, abandon);
    if (abandoned) await abandoned.close();
  }
  await waitForReopen(page);
  const reopened = await launch(page);
  try {
    await paintThroughViewer(reopened);
  } finally {
    await reopened.close();
  }
  await waitForReopen(page);
  return {real_canvas_changed_after_pointer_input_twice:true, owner_button_disabled_while_attached:true, competing_viewer_disconnected_while_original_controls:true,
    close_abandon_and_reopen:true};
}
