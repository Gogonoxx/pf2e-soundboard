/**
 * PF2E Soundboard
 *
 * 5 Tabs: Voices (VoiceMod Phase 2), SFX (manual), Ambience, Mood, Themes
 * Ambience/Mood/Themes load from sound-manifest.json (generated from OneDrive folder).
 * SFX tab is manually managed via game.settings.
 * All sounds broadcast to connected players via socket.
 * Audio files served through Cloudflare Worker proxy (same as map-browser).
 *
 * Uses native HTML5 Audio elements instead of Foundry Sound API to avoid
 * CORS issues with external URLs (Foundry's Sound connects to AudioContext
 * which requires CORS headers for cross-origin audio).
 */

const MODULE_ID = 'pf2e-soundboard';

// ============================================================================
// Utility
// ============================================================================

function generateId(prefix = 'snd') {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

function buildSoundUrl(manifest, soundPath) {
  const basePath = manifest.onedrive_base_path;
  const fullPath = basePath ? `${basePath}/${soundPath}` : soundPath;
  const encoded = fullPath.split('/').map(p => encodeURIComponent(p)).join('/');
  return `${manifest.worker_base_url}/${encoded}?share=music&proxy=true`;
}

/**
 * Convert a linear slider value (0-1) to actual audio volume.
 * The source tracks are mastered at 0 dBFS (full blast), way too loud for
 * background ambience during a voice chat session. We cap the maximum at 15%
 * and apply a cubic curve so the full slider range is usable.
 *
 * Slider 0%   → volume 0.000  (silent)
 * Slider 25%  → volume 0.002  (barely audible)
 * Slider 50%  → volume 0.019  (quiet background)
 * Slider 75%  → volume 0.063  (comfortable ambience)
 * Slider 100% → volume 0.150  (max, still reasonable)
 */
const VOLUME_MAX = 0.15;
function sliderToVolume(linear) {
  const clamped = Math.max(0, Math.min(1, linear));
  return clamped * clamped * clamped * VOLUME_MAX;
}

const FADE_DURATION = 5000; // 5 seconds fade in/out
const FADE_STEP = 50;       // Update every 50ms

function fadeIn(audio, targetVolume, duration = FADE_DURATION) {
  audio.volume = 0;
  const steps = duration / FADE_STEP;
  const increment = targetVolume / steps;
  let current = 0;
  const interval = setInterval(() => {
    current += increment;
    if (current >= targetVolume) {
      audio.volume = targetVolume;
      clearInterval(interval);
    } else {
      audio.volume = current;
    }
  }, FADE_STEP);
  return interval;
}

function fadeOut(audio, duration = FADE_DURATION) {
  return new Promise(resolve => {
    const startVolume = audio.volume;
    if (startVolume <= 0) { resolve(); return; }
    const steps = duration / FADE_STEP;
    const decrement = startVolume / steps;
    let current = startVolume;
    const interval = setInterval(() => {
      current -= decrement;
      if (current <= 0) {
        audio.volume = 0;
        clearInterval(interval);
        resolve();
      } else {
        audio.volume = current;
      }
    }, FADE_STEP);
  });
}

// ============================================================================
// Audio Cache (Browser Cache API)
// Caches audio files after first play → subsequent plays are instant.
// ============================================================================

const AUDIO_CACHE_NAME = 'pf2e-soundboard-audio-v1';

/**
 * Check if an audio URL is already cached. Returns a blob URL if cached, null otherwise.
 */
async function getCachedAudioUrl(src) {
  try {
    const cache = await caches.open(AUDIO_CACHE_NAME);
    const resp = await cache.match(src);
    if (resp) {
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      console.debug(`${MODULE_ID} | Cache HIT: ${decodeURIComponent(src.split('/').pop().split('?')[0])}`);
      return url;
    }
  } catch (e) { /* Cache API not available */ }
  return null;
}

/**
 * Cache an audio URL in the background (non-blocking).
 * Called after first play so subsequent plays are instant.
 */
function cacheAudioInBackground(src) {
  (async () => {
    try {
      const cache = await caches.open(AUDIO_CACHE_NAME);
      const existing = await cache.match(src);
      if (existing) return;

      const name = decodeURIComponent(src.split('/').pop().split('?')[0]);
      console.debug(`${MODULE_ID} | Caching: ${name}`);
      const response = await fetch(src);
      if (response.ok) {
        await cache.put(src, response);
        console.debug(`${MODULE_ID} | Cached: ${name}`);
      }
    } catch (e) { /* ignore */ }
  })();
}

/**
 * Prefetch an audio URL into the cache (e.g. on hover).
 * Returns immediately, caching happens in background.
 */
const _prefetchInFlight = new Set();
function prefetchAudio(src) {
  if (_prefetchInFlight.has(src)) return;
  _prefetchInFlight.add(src);
  (async () => {
    try {
      const cache = await caches.open(AUDIO_CACHE_NAME);
      const existing = await cache.match(src);
      if (existing) { _prefetchInFlight.delete(src); return; }

      const response = await fetch(src);
      if (response.ok) {
        await cache.put(src, response);
        console.debug(`${MODULE_ID} | Prefetched: ${decodeURIComponent(src.split('/').pop().split('?')[0])}`);
      }
    } catch (e) { /* ignore */ }
    _prefetchInFlight.delete(src);
  })();
}

// ============================================================================
// VoiceMod Manager — WebSocket Client for VoiceMod Control API
// ============================================================================

class VoiceModManager {
  static WEBSOCKET_URL = 'ws://localhost:59129/v1/';

  #ws = null;
  #status = 'disconnected'; // disconnected | connecting | connected | error
  #voices = [];
  #currentVoiceId = null;
  #voiceChangerEnabled = false;
  #pendingRequests = new Map(); // id -> { resolve, reject, timeout }
  #reconnectTimer = null;
  #onUpdate = null; // Callback when state changes (receives reason string)
  #voiceBitmaps = new Map(); // voiceID -> data:image/png;base64,... URI
  #bitmapPaused = false; // Pause bitmap loading when voice commands are in flight
  #hearMyself = false;
  #backgroundEnabled = false;

  constructor(onUpdate) {
    this.#onUpdate = onUpdate;
  }

  get status() { return this.#status; }
  get voices() { return this.#voices; }
  get currentVoiceId() { return this.#currentVoiceId; }
  get voiceChangerEnabled() { return this.#voiceChangerEnabled; }
  get isConnected() { return this.#status === 'connected'; }
  get voiceBitmaps() { return this.#voiceBitmaps; }
  get hearMyself() { return this.#hearMyself; }
  get backgroundEnabled() { return this.#backgroundEnabled; }

  // --- Connection ---

  connect() {
    const apiKey = game.settings.get(MODULE_ID, 'voicemodApiKey');
    if (!apiKey) {
      console.warn(`${MODULE_ID} | VoiceMod: No API key configured`);
      ui.notifications.warn('VoiceMod: No API key set. Configure it in Module Settings.');
      this.#status = 'error';
      this.#notify();
      return;
    }

    if (this.#ws?.readyState === WebSocket.OPEN || this.#ws?.readyState === WebSocket.CONNECTING) {
      return;
    }

    this.#status = 'connecting';
    this.#notify();

    // Try ws:// first — browsers allow ws://localhost from HTTPS pages
    // (localhost is a "potentially trustworthy origin")
    this.#tryConnect(apiKey, 'ws://localhost:59129/v1/');
  }

  #tryConnect(apiKey, url) {
    console.log(`${MODULE_ID} | VoiceMod: Connecting to ${url}...`);

    try {
      this.#ws = new WebSocket(url);
    } catch (err) {
      console.error(`${MODULE_ID} | VoiceMod WebSocket creation failed for ${url}:`, err);
      ui.notifications.error(`VoiceMod: Could not create WebSocket. ${err.message}`);
      this.#status = 'error';
      this.#notify();
      return;
    }

    this.#ws.onopen = () => {
      console.log(`${MODULE_ID} | VoiceMod WebSocket connected to ${url}, registering with key "${apiKey.substring(0, 12)}..."`);
      this.#send('registerClient', { clientKey: apiKey }).then(async (result) => {
        // Log the FULL response so we can see VoiceMod's actual format
        console.log(`${MODULE_ID} | VoiceMod registerClient FULL response:`, result);

        // Check for success — VoiceMod response format varies by version
        const statusCode = result?.actionObject?.statusCode
          ?? result?.payload?.statusCode
          ?? result?.statusCode;
        const isError = statusCode === 401 || statusCode === 403 || result?.error;
        // If no explicit error, treat as success (VoiceMod may omit statusCode)
        const isSuccess = statusCode === 200 || (!isError && result?.actionType);

        if (isSuccess) {
          console.log(`${MODULE_ID} | VoiceMod registered successfully!`);
          ui.notifications.info('VoiceMod: Connected!');
          this.#status = 'connected';
          this.#notify();
          await this.#fetchVoices();
          await this.#fetchStatus();
        } else {
          const msg = statusCode === 401 ? 'Invalid API key' : `Registration failed. Check console (F12) for details.`;
          console.warn(`${MODULE_ID} | VoiceMod registration failed. Full response:`, JSON.stringify(result, null, 2));
          ui.notifications.error(`VoiceMod: ${msg}`);
          this.#status = 'error';
          this.#notify();
          this.#ws.close();
        }
      }).catch(err => {
        console.warn(`${MODULE_ID} | VoiceMod registration error:`, err);
        ui.notifications.error(`VoiceMod: ${err.message}`);
        this.#status = 'error';
        this.#notify();
      });
    };

    this.#ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        this.#handleMessage(msg);
      } catch (err) {
        console.warn(`${MODULE_ID} | VoiceMod parse error:`, err);
      }
    };

    this.#ws.onclose = (event) => {
      console.log(`${MODULE_ID} | VoiceMod WebSocket closed (code: ${event.code}, reason: ${event.reason || 'none'})`);
      const wasConnected = this.#status === 'connected';
      this.#status = 'disconnected';
      this.#ws = null;
      this.#notify();

      // Auto-reconnect if we were connected before
      if (wasConnected) {
        this.#reconnectTimer = setTimeout(() => this.connect(), 5000);
      }
    };

    this.#ws.onerror = (event) => {
      console.error(`${MODULE_ID} | VoiceMod WebSocket error. Is VoiceMod desktop app running?`, event);
      if (this.#status === 'connecting') {
        ui.notifications.error('VoiceMod: Connection failed. Is VoiceMod running?');
      }
      // onclose will fire after this
    };
  }

  disconnect() {
    clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = null;
    if (this.#ws) {
      this.#ws.close();
      this.#ws = null;
    }
    this.#status = 'disconnected';
    this.#voices = [];
    this.#currentVoiceId = null;
    this.#voiceChangerEnabled = false;
    this.#hearMyself = false;
    this.#backgroundEnabled = false;
    this.#voiceBitmaps.clear();
    this.#notify();
  }

  // --- API Methods (fire-and-forget with optimistic state) ---

  async loadVoice(voiceId) {
    // Optimistic: update state IMMEDIATELY, don't wait for response
    this.#currentVoiceId = voiceId;
    // Pause bitmap loading so this command gets through immediately
    this.#bitmapPaused = true;
    this.#send('loadVoice', { voiceID: voiceId })
      .catch(err => console.warn(`${MODULE_ID} | VoiceMod loadVoice failed:`, err))
      .finally(() => { this.#bitmapPaused = false; });
  }

  async toggleVoiceChanger() {
    this.#voiceChangerEnabled = !this.#voiceChangerEnabled;
    this.#bitmapPaused = true;
    this.#send('toggleVoiceChanger', {})
      .catch(err => console.warn(`${MODULE_ID} | VoiceMod toggleVoiceChanger failed:`, err))
      .finally(() => { this.#bitmapPaused = false; });
  }

  async toggleHearMyVoice() {
    this.#hearMyself = !this.#hearMyself;
    this.#bitmapPaused = true;
    this.#send('toggleHearMyVoice', {})
      .catch(err => console.warn(`${MODULE_ID} | VoiceMod toggleHearMyVoice failed:`, err))
      .finally(() => { this.#bitmapPaused = false; });
  }

  async toggleBackground() {
    this.#backgroundEnabled = !this.#backgroundEnabled;
    this.#bitmapPaused = true;
    this.#send('toggleBackground', {})
      .catch(err => console.warn(`${MODULE_ID} | VoiceMod toggleBackground failed:`, err))
      .finally(() => { this.#bitmapPaused = false; });
  }

  async selectRandomVoice() {
    const result = await this.#send('selectRandomVoice', { mode: 'AllVoices' });
    if (result?.actionObject?.voiceID) {
      this.#currentVoiceId = result.actionObject.voiceID;
      this.#notify();
    }
    return result;
  }

  async loadBitmaps() {
    // Load bitmaps ONE AT A TIME to avoid flooding the WebSocket
    // Voice commands (loadVoice, toggleVoiceChanger) pause this via #bitmapPaused
    let loaded = 0;
    for (const v of this.#voices) {
      if (this.#status !== 'connected') return;
      // Yield to voice commands — wait while paused
      while (this.#bitmapPaused) {
        await new Promise(r => setTimeout(r, 100));
      }
      const id = v.voiceID || v.id;
      if (this.#voiceBitmaps.has(id)) continue;
      try {
        const result = await this.#send('getBitmap', { voiceID: id });
        // Log first response to debug format
        if (loaded === 0) {
          console.log(`${MODULE_ID} | VoiceMod getBitmap sample response keys:`,
            result?.actionObject ? Object.keys(result.actionObject) : 'no actionObject',
            result?.actionObject);
        }
        // Response format: actionObject.result.{default,selected,transparent}
        const bitmapResult = result?.actionObject?.result;
        const base64 = bitmapResult?.default
          ?? bitmapResult?.transparent
          ?? bitmapResult?.selected
          ?? result?.actionObject?.default;
        if (base64) {
          this.#voiceBitmaps.set(id, `data:image/png;base64,${base64}`);
          loaded++;
          if (loaded % 10 === 0) this.#notify('bitmaps');
        }
      } catch (err) { /* skip individual bitmap failures */ }
      // Small delay between requests
      await new Promise(r => setTimeout(r, 30));
    }
    this.#notify('bitmaps');
    console.log(`${MODULE_ID} | VoiceMod loaded ${this.#voiceBitmaps.size}/${this.#voices.length} bitmaps`);
  }

  // --- Internal ---

  async #fetchVoices() {
    try {
      const result = await this.#send('getVoices', {});
      console.log(`${MODULE_ID} | VoiceMod getVoices response:`, result);

      // Try multiple possible response formats
      const voices = result?.actionObject?.voices
        ?? result?.payload?.voices
        ?? result?.voices;

      if (voices && Array.isArray(voices)) {
        this.#voices = voices;
        console.log(`${MODULE_ID} | VoiceMod loaded ${voices.length} voices`);
        if (voices[0]) console.log(`${MODULE_ID} | VoiceMod sample voice object keys:`, Object.keys(voices[0]), 'sample:', voices[0]);
      } else {
        // Maybe voices are the actionObject itself (array at top level)
        const ao = result?.actionObject;
        if (Array.isArray(ao)) {
          this.#voices = ao;
          console.log(`${MODULE_ID} | VoiceMod loaded ${ao.length} voices (from actionObject array)`);
        } else {
          console.warn(`${MODULE_ID} | VoiceMod: Could not find voices in response. Keys:`,
            result ? Object.keys(result) : 'null',
            'actionObject keys:', ao ? Object.keys(ao) : 'null');
        }
      }
      this.#notify();
    } catch (err) {
      console.error(`${MODULE_ID} | VoiceMod getVoices failed:`, err);
    }
  }

  async #fetchStatus() {
    try {
      const currentResult = await this.#send('getCurrentVoice', {});
      console.log(`${MODULE_ID} | VoiceMod getCurrentVoice response:`, currentResult);
      this.#currentVoiceId = currentResult?.actionObject?.voiceID
        ?? currentResult?.payload?.voiceID
        ?? null;
    } catch (err) {
      console.warn(`${MODULE_ID} | VoiceMod getCurrentVoice failed:`, err);
    }

    try {
      const statusResult = await this.#send('getVoiceChangerStatus', {});
      this.#voiceChangerEnabled = statusResult?.actionObject?.value
        ?? statusResult?.payload?.value
        ?? false;
    } catch (err) {
      console.warn(`${MODULE_ID} | VoiceMod getVoiceChangerStatus failed:`, err);
    }

    try {
      const hearResult = await this.#send('getHearMyselfStatus', {});
      this.#hearMyself = hearResult?.actionObject?.value ?? false;
    } catch (err) {
      console.warn(`${MODULE_ID} | VoiceMod getHearMyselfStatus failed:`, err);
    }

    try {
      const bgResult = await this.#send('getBackgroundEffectStatus', {});
      this.#backgroundEnabled = bgResult?.actionObject?.value ?? false;
    } catch (err) {
      console.warn(`${MODULE_ID} | VoiceMod getBackgroundEffectStatus failed:`, err);
    }

    this.#notify();
  }

  #send(action, payload = {}) {
    return new Promise((resolve, reject) => {
      if (!this.#ws || this.#ws.readyState !== WebSocket.OPEN) {
        reject(new Error('Not connected'));
        return;
      }

      const id = crypto.randomUUID();
      const timeout = setTimeout(() => {
        this.#pendingRequests.delete(id);
        reject(new Error(`Timeout: ${action}`));
      }, 10000);

      this.#pendingRequests.set(id, { resolve, reject, timeout });

      this.#ws.send(JSON.stringify({ action, id, payload }));
    });
  }

  #handleMessage(msg) {
    const id = msg.actionID || msg.id;

    // Check if this is a response to a pending request
    if (id && this.#pendingRequests.has(id)) {
      const { resolve, timeout } = this.#pendingRequests.get(id);
      clearTimeout(timeout);
      this.#pendingRequests.delete(id);
      resolve(msg);
      return;
    }

    // Handle unsolicited events (voice changed externally, etc.)
    if (msg.actionType === 'voiceChanged' && msg.actionObject?.voiceID) {
      this.#currentVoiceId = msg.actionObject.voiceID;
      this.#notify('voice-changed');
    }
    if (msg.actionType === 'voiceChangerEnabledChanged' && msg.actionObject) {
      this.#voiceChangerEnabled = msg.actionObject.value ?? this.#voiceChangerEnabled;
      this.#notify('changer-toggled');
    }
    if (msg.actionType === 'hearMyselfEnabledChanged' && msg.actionObject) {
      this.#hearMyself = msg.actionObject.value ?? this.#hearMyself;
      this.#notify('changer-toggled');
    }
    if (msg.actionType === 'backgroundEffectsEnabledChanged' && msg.actionObject) {
      this.#backgroundEnabled = msg.actionObject.value ?? this.#backgroundEnabled;
      this.#notify('changer-toggled');
    }
  }

  #notify(reason = 'update') {
    if (this.#onUpdate) this.#onUpdate(reason);
  }
}

// ============================================================================
// ApplicationV2 - Soundboard Window
// ============================================================================

const { ApplicationV2, HandlebarsApplicationMixin } = foundry.applications.api;

class SoundboardApp extends HandlebarsApplicationMixin(ApplicationV2) {
  static #instance = null;

  static get instance() {
    return this.#instance;
  }

  static DEFAULT_OPTIONS = {
    id: 'pf2e-soundboard',
    classes: ['pf2e-soundboard'],
    window: {
      title: 'Pathfinder Soundboard',
      icon: 'fas fa-volume-up',
      resizable: true
    },
    position: {
      width: 360,
      height: 700
    }
  };

  static PARTS = {
    main: {
      template: `modules/${MODULE_ID}/templates/soundboard.hbs`
    }
  };

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------

  #manifest = null;               // Loaded from sound-manifest.json
  #activeTab = 'themes';          // Default to themes (most used)
  #activeSubTab = 'atmosphaere';  // Sub-tab within themes
  #editMode = false;
  #expandedCategories = new Set(); // Categories are COLLAPSED by default
  #playingIds = new Set();        // IDs with active playback (for UI glow)
  #activeSounds = new Map();      // id -> { audio: HTMLAudioElement, src, name, icon, categoryId, tab, paused, loop }
  #currentTrackId = {};           // Per-tab: { mood: 'id', themes: 'id' }
  #searchQuery = '';
  #scrollPosition = 0;
  #voiceMod = null;              // VoiceModManager instance

  // Sub-tab definitions for the Themes tab
  static SUB_TABS = [
    { id: 'atmosphaere', label: 'Atmosphäre', icon: 'fas fa-city',           parents: ['Atmosphäre'] },
    { id: 'ereignisse',  label: 'Ereignisse', icon: 'fas fa-calendar-star',  parents: ['Ereignisse'] },
    { id: 'kampf',       label: 'Kampf',      icon: 'fas fa-swords',         parents: ['Kampf'] },
    { id: 'kultur',      label: 'Kultur',     icon: 'fas fa-landmark-dome',  parents: ['Rassen', 'Flusswald', 'Orfnir'] },
    { id: 'monster',     label: 'Monster',    icon: 'fas fa-skull',          parents: ['Monster'] }
  ];

  constructor(options = {}) {
    super(options);
    SoundboardApp.#instance = this;
    this.#voiceMod = new VoiceModManager((reason) => {
      if (!this.rendered || this.#activeTab !== 'voices') return;
      if (reason === 'voice-changed' || reason === 'changer-toggled') {
        // Skip re-render — optimistic DOM updates already handle this
        return;
      }
      if (reason === 'bitmaps') {
        this.#updateVoiceBitmapsDOM();
        return;
      }
      // Full re-render for connection changes, initial voice load, etc.
      this.render();
    });
  }

  // ---------------------------------------------------------------------------
  // VoiceMod DOM Updates (no re-render)
  // ---------------------------------------------------------------------------

  #updateVoiceBitmapsDOM() {
    const html = this.element;
    if (!html) return;
    const bitmaps = this.#voiceMod.voiceBitmaps;
    // Only update buttons that still have the icon placeholder (not yet replaced)
    html.querySelectorAll('.vm-voice-btn .vm-voice-icon').forEach(iconEl => {
      const btn = iconEl.closest('.vm-voice-btn');
      if (!btn) return;
      const voiceId = btn.dataset.voiceId;
      const bitmapUrl = bitmaps.get(voiceId);
      if (bitmapUrl) {
        const imgEl = document.createElement('img');
        imgEl.className = 'vm-voice-img';
        imgEl.src = bitmapUrl;
        imgEl.alt = '';
        iconEl.replaceWith(imgEl);
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Manifest Loading
  // ---------------------------------------------------------------------------

  async #loadManifest() {
    if (this.#manifest) return;
    try {
      const response = await fetch(`modules/${MODULE_ID}/data/sound-manifest.json`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      this.#manifest = await response.json();
      console.log(`${MODULE_ID} | Manifest loaded: ${this.#manifest.stats.total_files} files, ${this.#manifest.stats.total_categories} categories`);
    } catch (err) {
      console.error(`${MODULE_ID} | Failed to load manifest:`, err);
      this.#manifest = { tabs: {}, stats: { total_files: 0, total_categories: 0 } };
    }
  }

  // ---------------------------------------------------------------------------
  // Data Access
  // ---------------------------------------------------------------------------

  #getSfxData() {
    return game.settings.get(MODULE_ID, 'sfxData');
  }

  async #setSfxData(data) {
    await game.settings.set(MODULE_ID, 'sfxData', data);
  }

  #getFavorites() {
    return game.settings.get(MODULE_ID, 'favorites');
  }

  async #setFavorites(data) {
    await game.settings.set(MODULE_ID, 'favorites', data);
  }

  // ---------------------------------------------------------------------------
  // Get categories for the active tab
  // ---------------------------------------------------------------------------

  #getCategoriesForTab(tab, subTab = null) {
    if (tab === 'sfx') {
      const sfx = this.#getSfxData();
      return (sfx.categories || []).map(cat => ({
        ...cat,
        sounds: (cat.sounds || []).map(s => ({
          ...s,
          src: s.src
        }))
      }));
    }

    const manifestTab = this.#manifest?.tabs?.[tab];
    if (!manifestTab) return [];

    let categories = manifestTab.categories;

    // Filter by sub-tab for themes
    if (tab === 'themes' && subTab) {
      const subDef = SoundboardApp.SUB_TABS.find(s => s.id === subTab);
      if (subDef) {
        categories = categories.filter(cat => subDef.parents.includes(cat.parent));
      }
    }

    return categories.map(cat => ({
      ...cat,
      sounds: (cat.sounds || []).map(s => ({
        ...s,
        src: buildSoundUrl(this.#manifest, s.path)
      }))
    }));
  }

  // ---------------------------------------------------------------------------
  // Tab behavior config
  // ---------------------------------------------------------------------------

  #getTabBehavior(tab) {
    const behaviors = {
      sfx: 'oneshot',
      ambience: 'loop-multi',
      mood: 'loop-single',
      themes: 'loop-single'
    };
    return behaviors[tab] || 'oneshot';
  }

  // ---------------------------------------------------------------------------
  // Context Preparation
  // ---------------------------------------------------------------------------

  async _prepareContext(options) {
    await this.#loadManifest();

    const tab = this.#activeTab;
    const subTab = (tab === 'themes') ? this.#activeSubTab : null;
    let categories = this.#getCategoriesForTab(tab, subTab);

    // Apply search filter
    if (this.#searchQuery) {
      const q = this.#searchQuery.toLowerCase();
      categories = categories
        .map(cat => ({
          ...cat,
          sounds: cat.sounds.filter(s => s.name.toLowerCase().includes(q))
        }))
        .filter(cat => cat.sounds.length > 0);
    }

    // Apply collapsed state and playing/rating state
    const ratings = this.#getRatings();
    categories = categories.map(cat => ({
      ...cat,
      collapsed: !this.#expandedCategories.has(cat.id),
      sounds: cat.sounds.map(s => ({
        ...s,
        playing: this.#playingIds.has(s.id),
        rating: ratings[s.id] || 0
      })).sort((a, b) => (b.rating || 0) - (a.rating || 0))
    }));

    const favorites = (this.#getFavorites() || []).map((fav, index) => ({
      ...fav,
      slot: index,
      empty: !fav
    }));

    const behavior = this.#getTabBehavior(tab);

    // Now Playing: build list from all active sounds
    const nowPlayingTracks = [];
    for (const [id, entry] of this.#activeSounds) {
      nowPlayingTracks.push({
        soundId: id,
        name: entry.name || 'Unknown',
        icon: entry.icon || 'fas fa-music',
        src: entry.src,
        paused: entry.paused || false,
        loop: entry.audio?.loop || false,
        rating: ratings[id] || 0,
        volume: entry.sliderValue ?? game.settings.get(MODULE_ID, 'globalVolume')
      });
    }

    // Sub-tabs for themes
    const subTabs = (tab === 'themes') ? SoundboardApp.SUB_TABS.map(st => ({
      ...st,
      active: st.id === this.#activeSubTab
    })) : null;

    return {
      activeTab: tab,
      activeSubTab: this.#activeSubTab,
      editMode: this.#editMode,
      searchQuery: this.#searchQuery,
      categories,
      favorites,
      nowPlayingTracks,
      hasNowPlaying: nowPlayingTracks.length > 0 || (this.#voiceMod?.status === 'connected' && !!this.#voiceMod?.currentVoiceId),
      subTabs,
      isVoicesTab: tab === 'voices',
      voiceMod: {
        status: this.#voiceMod?.status || 'disconnected',
        enabled: this.#voiceMod?.voiceChangerEnabled || false,
        currentVoiceId: this.#voiceMod?.currentVoiceId || null,
        hearMyself: this.#voiceMod?.hearMyself || false,
        backgroundEnabled: this.#voiceMod?.backgroundEnabled || false,
        hasApiKey: !!game.settings.get(MODULE_ID, 'voicemodApiKey'),
        currentVoiceName: (() => {
          const vid = this.#voiceMod?.currentVoiceId;
          if (!vid) return null;
          const v = this.#voiceMod?.voices?.find(x => (x.voiceID || x.id) === vid);
          return v?.friendlyName || v?.name || null;
        })(),
        currentVoiceBitmap: this.#voiceMod?.voiceBitmaps?.get(this.#voiceMod?.currentVoiceId) || null,
        voices: (this.#voiceMod?.voices || [])
          .filter(v => v.isEnabled !== false && v.enabled !== false)
          .map(v => {
            const id = v.voiceID || v.id;
            return {
              id,
              name: v.friendlyName || v.name || 'Unknown',
              isFavorite: v.isFavorited || v.favorited || false,
              isActive: id === this.#voiceMod?.currentVoiceId,
              bitmapUrl: this.#voiceMod?.voiceBitmaps?.get(id) || null
            };
          })
      },
      isSfxTab: tab === 'sfx',
      isThemesTab: tab === 'themes',
      isManifestTab: ['ambience', 'mood', 'themes'].includes(tab),
      hasStopButton: behavior === 'loop-single' || behavior === 'loop-multi',
      tabBehavior: behavior,
      manifest: this.#manifest ? {
        totalFiles: this.#manifest.stats.total_files,
        workerUrl: this.#manifest.worker_base_url
      } : null
    };
  }

  // ---------------------------------------------------------------------------
  // Scroll Preservation
  // ---------------------------------------------------------------------------

  _preRender(context, options) {
    const container = this.element?.querySelector('.soundboard-content');
    if (container) {
      this.#scrollPosition = container.scrollTop;
    }
    return super._preRender(context, options);
  }

  // ---------------------------------------------------------------------------
  // Event Handling
  // ---------------------------------------------------------------------------

  _onRender(context, options) {
    const html = this.element;

    // Restore scroll
    const container = html.querySelector('.soundboard-content');
    if (container && this.#scrollPosition > 0) {
      requestAnimationFrame(() => {
        container.scrollTop = this.#scrollPosition;
      });
    }

    // --- Tab switching ---
    html.querySelectorAll('[data-action="switch-tab"]').forEach(el => {
      el.addEventListener('click', ev => {
        this.#activeTab = ev.currentTarget.dataset.tab;
        this.#searchQuery = '';
        this.render();
      });
    });

    // --- Sub-tab switching (themes) ---
    html.querySelectorAll('[data-action="switch-subtab"]').forEach(el => {
      el.addEventListener('click', ev => {
        this.#activeSubTab = ev.currentTarget.dataset.subtab;
        this.render();
      });
    });

    // --- Search ---
    const searchInput = html.querySelector('[data-action="search"]');
    if (searchInput) {
      let searchTimeout;
      searchInput.addEventListener('input', ev => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
          this.#searchQuery = ev.target.value;
          this.render();
        }, 200);
      });
    }

    // --- Toggle edit mode ---
    html.querySelectorAll('[data-action="toggle-edit"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#editMode = !this.#editMode;
        this.render();
      });
    });

    // --- Category collapse/expand ---
    html.querySelectorAll('[data-action="toggle-category"]').forEach(el => {
      el.addEventListener('click', ev => {
        if (ev.target.closest('.edit-btn') || ev.target.closest('.random-btn')) return;
        const catId = ev.currentTarget.dataset.categoryId;
        if (this.#expandedCategories.has(catId)) {
          this.#expandedCategories.delete(catId);
        } else {
          this.#expandedCategories.add(catId);
        }
        this.render();
      });
    });

    // --- Play random from category ---
    html.querySelectorAll('[data-action="play-random"]').forEach(el => {
      el.addEventListener('click', ev => {
        ev.stopPropagation();
        const catId = ev.currentTarget.dataset.categoryId;
        this.#playRandomFromCategory(catId);
      });
    });

    // --- Rate sound (upvote/downvote) ---
    html.querySelectorAll('[data-action="rate-sound"]').forEach(el => {
      el.addEventListener('click', ev => {
        ev.stopPropagation();
        const soundId = ev.currentTarget.dataset.soundId;
        const value = parseInt(ev.currentTarget.dataset.rateValue);
        this.#rateSound(soundId, value);
      });
    });

    // --- Add playing track to favorites ---
    html.querySelectorAll('[data-action="fav-playing"]').forEach(el => {
      el.addEventListener('click', ev => {
        ev.stopPropagation();
        const soundId = ev.currentTarget.dataset.soundId;
        const src = ev.currentTarget.dataset.src;
        const name = ev.currentTarget.dataset.soundName;
        const icon = ev.currentTarget.dataset.soundIcon;
        this.#showFavoriteDialog(soundId, name, src, icon);
      });
    });

    // --- Play sound ---
    html.querySelectorAll('[data-action="play-sound"]').forEach(el => {
      el.addEventListener('click', ev => {
        if (this.#editMode) return;
        const { src, soundId, soundName, soundIcon, categoryId } = ev.currentTarget.dataset;
        this.#handlePlay(soundId, src, { name: soundName, icon: soundIcon, categoryId });
      });

      // Hover prefetch: start caching audio after 200ms hover
      let hoverTimer = null;
      el.addEventListener('mouseenter', () => {
        const src = el.dataset.src;
        if (src) hoverTimer = setTimeout(() => prefetchAudio(src), 200);
      });
      el.addEventListener('mouseleave', () => {
        if (hoverTimer) { clearTimeout(hoverTimer); hoverTimer = null; }
      });
    });

    // --- Stop all ---
    html.querySelectorAll('[data-action="stop-all"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#stopAllInTab();
      });
    });

    // --- Play favorite ---
    html.querySelectorAll('[data-action="play-favorite"]').forEach(el => {
      el.addEventListener('click', ev => {
        const slot = parseInt(ev.currentTarget.dataset.slot);
        const favorites = this.#getFavorites();
        const fav = favorites?.[slot];
        if (!fav) return;
        if (fav.type === 'voice') {
          // Load VoiceMod voice
          this.#voiceMod?.loadVoice(fav.voiceId);
          ui.notifications.info(`Voice: ${fav.name}`);
        } else {
          this.#playWithBehavior(fav.id || `fav-${slot}`, fav.src, fav.behavior || 'oneshot', {
            name: fav.name, icon: fav.icon, categoryId: null
          });
        }
      });
    });

    // --- Drag-and-drop: sounds -> favorite slots ---
    html.querySelectorAll('.sound-btn[draggable="true"]').forEach(el => {
      el.addEventListener('dragstart', ev => {
        const data = {
          id: el.dataset.soundId,
          name: el.dataset.soundName,
          src: el.dataset.src,
          icon: el.dataset.soundIcon || 'fas fa-music',
          behavior: this.#getTabBehavior(this.#activeTab)
        };
        ev.dataTransfer.setData('application/pf2e-sound', JSON.stringify(data));
        ev.dataTransfer.effectAllowed = 'copy';
        el.classList.add('dragging');
      });
      el.addEventListener('dragend', ev => {
        el.classList.remove('dragging');
        html.querySelectorAll('.favorite-slot.drop-over').forEach(s => s.classList.remove('drop-over'));
      });
    });

    html.querySelectorAll('.favorite-slot').forEach(el => {
      el.addEventListener('dragover', ev => {
        if (ev.dataTransfer.types.includes('application/pf2e-sound')) {
          ev.preventDefault();
          ev.dataTransfer.dropEffect = 'copy';
        }
      });
      el.addEventListener('dragenter', ev => {
        if (ev.dataTransfer.types.includes('application/pf2e-sound')) {
          el.classList.add('drop-over');
        }
      });
      el.addEventListener('dragleave', ev => {
        if (!el.contains(ev.relatedTarget)) {
          el.classList.remove('drop-over');
        }
      });
      el.addEventListener('drop', async ev => {
        ev.preventDefault();
        el.classList.remove('drop-over');
        const raw = ev.dataTransfer.getData('application/pf2e-sound');
        if (!raw) return;
        try {
          const data = JSON.parse(raw);
          const slot = parseInt(el.dataset.slot);
          const favs = this.#getFavorites();
          if (data.type === 'voice') {
            favs[slot] = {
              type: 'voice',
              id: data.id,
              voiceId: data.voiceId,
              name: data.name,
              icon: data.icon || 'fas fa-microphone',
              color: '#8b2020'
            };
          } else {
            favs[slot] = {
              id: data.id,
              name: data.name,
              src: data.src,
              icon: data.icon,
              color: '#c0392b',
              behavior: data.behavior
            };
          }
          await this.#setFavorites(favs);
          this.render();
          ui.notifications.info(`"${data.name}" \u2192 Slot ${slot + 1}`);
        } catch (err) {
          console.warn(`${MODULE_ID} | Drop failed:`, err);
        }
      });
    });

    // --- Right-click on favorite: remove ---
    html.querySelectorAll('.favorite-slot:not(.empty-slot)').forEach(el => {
      el.addEventListener('contextmenu', async ev => {
        ev.preventDefault();
        const slot = parseInt(ev.currentTarget.dataset.slot);
        await this.#removeFavorite(slot);
      });
    });

    // --- Now Playing controls (per-track) ---
    html.querySelectorAll('[data-action="np-pause"]').forEach(el => {
      el.addEventListener('click', () => {
        const soundId = el.dataset.soundId;
        this.#pauseResumeTrack(soundId);
      });
    });
    html.querySelectorAll('[data-action="np-stop"]').forEach(el => {
      el.addEventListener('click', () => {
        const soundId = el.dataset.soundId;
        this.#stopTrack(soundId);
      });
    });
    html.querySelectorAll('[data-action="np-loop"]').forEach(el => {
      el.addEventListener('click', () => {
        const soundId = el.dataset.soundId;
        this.#toggleTrackLoop(soundId);
      });
    });
    html.querySelectorAll('[data-action="np-rate"]').forEach(el => {
      el.addEventListener('click', () => {
        const soundId = el.dataset.soundId;
        const value = parseInt(el.dataset.rateValue);
        this.#rateSound(soundId, value);
      });
    });
    html.querySelectorAll('[data-action="np-fav"]').forEach(el => {
      el.addEventListener('click', () => {
        const soundId = el.dataset.soundId;
        const entry = this.#activeSounds.get(soundId);
        if (entry) {
          this.#showFavoriteDialog(soundId, entry.name, entry.src, entry.icon);
        }
      });
    });
    html.querySelectorAll('[data-action="np-prev"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#playAdjacentTrack(el.dataset.soundId, -1);
      });
    });
    html.querySelectorAll('[data-action="np-next"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#playAdjacentTrack(el.dataset.soundId, 1);
      });
    });
    html.querySelectorAll('[data-action="np-volume"]').forEach(el => {
      el.addEventListener('input', (ev) => {
        const soundId = el.dataset.soundId;
        const sliderVal = parseFloat(ev.target.value);
        const vol = sliderToVolume(sliderVal);
        const entry = this.#activeSounds.get(soundId);
        if (entry?.audio) {
          entry.audio.volume = vol;
          entry.sliderValue = sliderVal;
          // Broadcast volume change to clients (send actual volume, not slider value)
          game.socket.emit(`module.${MODULE_ID}`, {
            action: 'volume', src: entry.src, volume: vol
          });
        }
      });
    });

    // =========================================================================
    // VOICEMOD CONTROLS
    // =========================================================================

    html.querySelectorAll('[data-action="vm-connect"]').forEach(el => {
      el.addEventListener('click', () => this.#voiceMod.connect());
    });
    html.querySelectorAll('[data-action="vm-disconnect"]').forEach(el => {
      el.addEventListener('click', () => this.#voiceMod.disconnect());
    });
    html.querySelectorAll('[data-action="vm-toggle-changer"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#voiceMod.toggleVoiceChanger();
        // Instant DOM update — no re-render
        const isOn = this.#voiceMod.voiceChangerEnabled;
        el.classList.toggle('active', isOn);
        el.innerHTML = `<i class="fas fa-${isOn ? 'microphone' : 'microphone-slash'}"></i> ${isOn ? 'ON' : 'OFF'}`;
      });
    });
    html.querySelectorAll('[data-action="vm-random-voice"]').forEach(el => {
      el.addEventListener('click', () => this.#voiceMod.selectRandomVoice());
    });
    html.querySelectorAll('[data-action="vm-load-voice"]').forEach(el => {
      el.addEventListener('click', () => {
        const voiceId = el.dataset.voiceId;
        this.#voiceMod.loadVoice(voiceId);
        // Instant DOM update — swap active class without re-render
        html.querySelectorAll('.vm-voice-btn.active').forEach(b => b.classList.remove('active'));
        el.classList.add('active');
      });
    });

    // --- VoiceMod Now Playing controls ---
    html.querySelectorAll('[data-action="vm-np-toggle"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#voiceMod.toggleVoiceChanger();
        el.classList.toggle('np-btn-active', this.#voiceMod.voiceChangerEnabled);
      });
    });
    html.querySelectorAll('[data-action="vm-np-hear"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#voiceMod.toggleHearMyVoice();
        el.classList.toggle('np-btn-active', this.#voiceMod.hearMyself);
      });
    });
    html.querySelectorAll('[data-action="vm-np-background"]').forEach(el => {
      el.addEventListener('click', () => {
        this.#voiceMod.toggleBackground();
        el.classList.toggle('np-btn-active', this.#voiceMod.backgroundEnabled);
      });
    });

    // --- Voice button drag to favorites ---
    html.querySelectorAll('.vm-voice-btn[data-voice-id]').forEach(el => {
      el.setAttribute('draggable', 'true');
      el.addEventListener('dragstart', ev => {
        const voiceId = el.dataset.voiceId;
        const name = el.querySelector('.vm-voice-name')?.textContent || 'Voice';
        const data = {
          type: 'voice',
          id: `voice-${voiceId}`,
          voiceId,
          name,
          icon: 'fas fa-microphone'
        };
        ev.dataTransfer.setData('application/pf2e-sound', JSON.stringify(data));
        ev.dataTransfer.effectAllowed = 'copy';
        el.classList.add('dragging');
      });
      el.addEventListener('dragend', () => {
        el.classList.remove('dragging');
        html.querySelectorAll('.favorite-slot.drop-over').forEach(s => s.classList.remove('drop-over'));
      });
    });

    // Trigger bitmap loading when voices tab is shown and connected
    if (this.#activeTab === 'voices' && this.#voiceMod?.isConnected
        && this.#voiceMod.voices.length > 0 && this.#voiceMod.voiceBitmaps.size === 0) {
      this.#voiceMod.loadBitmaps();
    }

    // =========================================================================
    // EDIT MODE ACTIONS (SFX tab only)
    // =========================================================================

    html.querySelectorAll('[data-action="add-sound"]').forEach(el => {
      el.addEventListener('click', ev => {
        ev.stopPropagation();
        const catId = ev.currentTarget.dataset.categoryId;
        this.#showAddSoundDialog(catId);
      });
    });

    html.querySelectorAll('[data-action="delete-sound"]').forEach(el => {
      el.addEventListener('click', async ev => {
        ev.stopPropagation();
        const catId = ev.currentTarget.dataset.categoryId;
        const soundId = ev.currentTarget.dataset.soundId;
        const confirmed = await Dialog.confirm({
          title: 'Delete Sound',
          content: '<p>Really delete this sound?</p>'
        });
        if (confirmed) await this.#removeSfxSound(catId, soundId);
      });
    });

    html.querySelectorAll('[data-action="edit-sound"]').forEach(el => {
      el.addEventListener('click', ev => {
        ev.stopPropagation();
        const catId = ev.currentTarget.dataset.categoryId;
        const soundId = ev.currentTarget.dataset.soundId;
        this.#showEditSoundDialog(catId, soundId);
      });
    });

    html.querySelectorAll('[data-action="add-category"]').forEach(el => {
      el.addEventListener('click', () => this.#showAddCategoryDialog());
    });

    html.querySelectorAll('[data-action="delete-category"]').forEach(el => {
      el.addEventListener('click', async ev => {
        ev.stopPropagation();
        const catId = ev.currentTarget.dataset.categoryId;
        const confirmed = await Dialog.confirm({
          title: 'Delete Category',
          content: '<p>Delete this category and all its sounds?</p>'
        });
        if (confirmed) await this.#removeSfxCategory(catId);
      });
    });

    html.querySelectorAll('[data-action="remove-favorite"]').forEach(el => {
      el.addEventListener('click', async ev => {
        ev.stopPropagation();
        const slot = parseInt(ev.currentTarget.dataset.slot);
        await this.#removeFavorite(slot);
      });
    });
  }

  // ---------------------------------------------------------------------------
  // Playback Router
  // ---------------------------------------------------------------------------

  #handlePlay(soundId, src, meta = {}) {
    const behavior = this.#getTabBehavior(this.#activeTab);
    this.#playWithBehavior(soundId, src, behavior, {
      ...meta,
      tab: this.#activeTab
    });
  }

  #playWithBehavior(soundId, src, behavior, meta = {}) {
    switch (behavior) {
      case 'oneshot':
        this.#playOneshot(soundId, src);
        break;
      case 'loop-single':
        this.#toggleLoopSingle(soundId, src, meta);
        break;
      case 'loop-multi':
        this.#toggleLoopMulti(soundId, src, meta);
        break;
    }
  }

  // ---------------------------------------------------------------------------
  // Playback: One-shot (SFX)
  // Uses native Audio element + manual socket broadcast
  // ---------------------------------------------------------------------------

  async #playOneshot(soundId, src) {
    const sliderVal = game.settings.get(MODULE_ID, 'globalVolume');
    const volume = sliderToVolume(sliderVal);

    this.#playingIds.add(soundId);
    this.render();

    // Check browser cache first → instant if cached
    const cachedUrl = await getCachedAudioUrl(src);
    const audio = new Audio(cachedUrl || src);
    audio.volume = volume;
    audio.play().catch(err => {
      console.warn(`${MODULE_ID} | Oneshot playback failed:`, err);
      ui.notifications.warn('Audio playback failed');
    });

    // Cache in background for next time
    if (!cachedUrl) cacheAudioInBackground(src);

    // Broadcast to all other clients (always use original URL)
    game.socket.emit(`module.${MODULE_ID}`, {
      action: 'play', src, volume, loop: false
    });

    setTimeout(() => {
      this.#playingIds.delete(soundId);
      if (this.rendered) this.render();
    }, 600);
  }

  // ---------------------------------------------------------------------------
  // Playback: Loop-Single (Mood, Themes — one track per tab)
  // Uses native Audio element
  // ---------------------------------------------------------------------------

  async #toggleLoopSingle(soundId, src, meta = {}) {
    const tab = meta.tab || this.#activeTab;
    const sliderVal = game.settings.get(MODULE_ID, 'globalVolume');
    const volume = sliderToVolume(sliderVal);
    const currentId = this.#currentTrackId[tab];

    // If same track -> fade out and stop
    if (currentId === soundId) {
      const entry = this.#activeSounds.get(soundId);
      if (entry?.audio) {
        fadeOut(entry.audio).then(() => this.#stopSound(soundId));
      } else {
        this.#stopSound(soundId);
      }
      this.#playingIds.delete(soundId);
      this.#currentTrackId[tab] = null;
      if (entry) game.socket.emit(`module.${MODULE_ID}`, { action: 'stop', src: entry.src });
      this.render();
      return;
    }

    // Fade out current track in this tab, then stop
    if (currentId) {
      const current = this.#activeSounds.get(currentId);
      if (current?.audio) {
        fadeOut(current.audio).then(() => {
          try { current.audio.pause(); current.audio.src = ''; } catch (e) { /* */ }
          this.#activeSounds.delete(currentId);
        });
        game.socket.emit(`module.${MODULE_ID}`, { action: 'stop', src: current.src });
      }
      this.#playingIds.delete(currentId);
    }

    // Play new track — check cache first, then native Audio with fade in
    const cachedUrl = await getCachedAudioUrl(src);
    const audio = new Audio(cachedUrl || src);
    audio.volume = 0; // Start silent, fade in
    audio.loop = true;

    try {
      await audio.play();
      fadeIn(audio, volume); // Fade in over 5 seconds
    } catch (err) {
      console.warn(`${MODULE_ID} | Loop playback failed:`, err);
      ui.notifications.warn('Audio playback failed');
      return;
    }

    // Cache in background for next time
    if (!cachedUrl) cacheAudioInBackground(src);

    this.#activeSounds.set(soundId, {
      audio, src,
      name: meta.name, icon: meta.icon,
      categoryId: meta.categoryId, tab,
      paused: false, loop: true,
      sliderValue: sliderVal
    });
    this.#currentTrackId[tab] = soundId;
    this.#playingIds.add(soundId);

    game.socket.emit(`module.${MODULE_ID}`, {
      action: 'play', src, volume, loop: true
    });

    this.render();
  }

  // ---------------------------------------------------------------------------
  // Playback: Loop-Multi (Ambience — multiple simultaneous)
  // ---------------------------------------------------------------------------

  async #toggleLoopMulti(soundId, src, meta = {}) {
    const sliderVal = game.settings.get(MODULE_ID, 'globalVolume');
    const volume = sliderToVolume(sliderVal);

    // Toggle off — fade out
    if (this.#activeSounds.has(soundId)) {
      const entry = this.#activeSounds.get(soundId);
      this.#playingIds.delete(soundId);
      if (entry?.audio) {
        fadeOut(entry.audio).then(() => {
          try { entry.audio.pause(); entry.audio.src = ''; } catch (e) { /* */ }
          this.#activeSounds.delete(soundId);
        });
      } else {
        this.#activeSounds.delete(soundId);
      }
      game.socket.emit(`module.${MODULE_ID}`, { action: 'stop', src });
      this.render();
      return;
    }

    // Start loop — check cache first, then native Audio with fade in
    const cachedUrl = await getCachedAudioUrl(src);
    const audio = new Audio(cachedUrl || src);
    audio.volume = 0;
    audio.loop = true;

    try {
      await audio.play();
      fadeIn(audio, volume);
    } catch (err) {
      console.warn(`${MODULE_ID} | Ambience playback failed:`, err);
      ui.notifications.warn('Audio playback failed');
      return;
    }

    // Cache in background for next time
    if (!cachedUrl) cacheAudioInBackground(src);

    this.#activeSounds.set(soundId, {
      audio, src,
      name: meta.name, icon: meta.icon,
      categoryId: meta.categoryId, tab: meta.tab || 'ambience',
      paused: false, loop: true,
      sliderValue: sliderVal
    });
    this.#playingIds.add(soundId);

    game.socket.emit(`module.${MODULE_ID}`, {
      action: 'play', src, volume, loop: true
    });

    this.render();
  }

  // ---------------------------------------------------------------------------
  // Stop Helpers
  // ---------------------------------------------------------------------------

  #stopSound(soundId) {
    const entry = this.#activeSounds.get(soundId);
    if (entry?.audio) {
      try {
        entry.audio.pause();
        entry.audio.currentTime = 0;
        entry.audio.src = ''; // Release resources
      } catch (e) {
        console.warn(`${MODULE_ID} | Failed to stop sound:`, e);
      }
    }
    this.#activeSounds.delete(soundId);
  }

  #stopAllInTab() {
    const tab = this.#activeTab;

    // For loop-single tabs, stop the current track
    if (this.#currentTrackId[tab]) {
      const id = this.#currentTrackId[tab];
      const entry = this.#activeSounds.get(id);
      if (entry) {
        this.#stopSound(id);
        game.socket.emit(`module.${MODULE_ID}`, { action: 'stop', src: entry.src });
      }
      this.#playingIds.delete(id);
      this.#currentTrackId[tab] = null;
    }

    // For loop-multi (ambience), stop all active sounds
    if (this.#getTabBehavior(tab) === 'loop-multi') {
      for (const [id, entry] of this.#activeSounds) {
        if (entry.audio) {
          try { entry.audio.pause(); entry.audio.src = ''; } catch (e) { /* */ }
        }
        game.socket.emit(`module.${MODULE_ID}`, { action: 'stop', src: entry.src });
      }
      this.#activeSounds.clear();
      this.#playingIds.clear();
    }

    this.render();
  }

  // ---------------------------------------------------------------------------
  // Now Playing Controls (per-track)
  // ---------------------------------------------------------------------------

  #pauseResumeTrack(soundId) {
    const entry = this.#activeSounds.get(soundId);
    if (!entry?.audio) return;

    if (entry.paused) {
      entry.audio.play();
      entry.paused = false;
      game.socket.emit(`module.${MODULE_ID}`, { action: 'resume', src: entry.src });
    } else {
      entry.audio.pause();
      entry.paused = true;
      game.socket.emit(`module.${MODULE_ID}`, { action: 'pause', src: entry.src });
    }
    this.render();
  }

  #stopTrack(soundId) {
    const entry = this.#activeSounds.get(soundId);
    if (!entry) return;
    const { src, tab } = entry;
    this.#playingIds.delete(soundId);
    if (tab && this.#currentTrackId[tab] === soundId) {
      this.#currentTrackId[tab] = null;
    }
    // Fade out, then clean up
    if (entry.audio) {
      fadeOut(entry.audio).then(() => {
        try { entry.audio.pause(); entry.audio.src = ''; } catch (e) { /* */ }
        this.#activeSounds.delete(soundId);
      });
    } else {
      this.#activeSounds.delete(soundId);
    }
    game.socket.emit(`module.${MODULE_ID}`, { action: 'stop', src });
    this.render();
  }

  #toggleTrackLoop(soundId) {
    const entry = this.#activeSounds.get(soundId);
    if (!entry?.audio) return;
    entry.audio.loop = !entry.audio.loop;
    entry.loop = entry.audio.loop;
    this.render();
  }

  #playAdjacentTrack(soundId, direction) {
    const entry = this.#activeSounds.get(soundId);
    if (!entry?.categoryId || !entry?.tab) return;

    const subTab = (entry.tab === 'themes') ? this.#activeSubTab : null;
    const categories = this.#getCategoriesForTab(entry.tab, subTab);
    const cat = categories.find(c => c.id === entry.categoryId);
    if (!cat?.sounds?.length) return;

    const currentIndex = cat.sounds.findIndex(s => s.id === soundId);
    if (currentIndex === -1) return;

    const nextIndex = (currentIndex + direction + cat.sounds.length) % cat.sounds.length;
    const nextSound = cat.sounds[nextIndex];

    this.#playWithBehavior(nextSound.id, nextSound.src, this.#getTabBehavior(entry.tab), {
      name: nextSound.name,
      icon: nextSound.icon || 'fas fa-music',
      categoryId: entry.categoryId,
      tab: entry.tab
    });
  }

  // ---------------------------------------------------------------------------
  // SFX Sound Management (Editor)
  // ---------------------------------------------------------------------------

  #showAddSoundDialog(categoryId) {
    new Dialog({
      title: 'Add Sound',
      content: `
        <form class="soundboard-dialog">
          <div class="form-group">
            <label>Name</label>
            <input type="text" name="name" placeholder="Longsword Slash">
          </div>
          <div class="form-group">
            <label>URL</label>
            <input type="text" name="src" placeholder="https://...">
          </div>
          <div class="form-group">
            <label>Icon (FontAwesome)</label>
            <input type="text" name="icon" value="fas fa-music">
          </div>
        </form>
      `,
      buttons: {
        save: {
          icon: '<i class="fas fa-save"></i>',
          label: 'Add',
          callback: async (html) => {
            const name = html.find('[name="name"]').val().trim();
            const src = html.find('[name="src"]').val().trim();
            const icon = html.find('[name="icon"]').val().trim() || 'fas fa-music';
            if (!name || !src) return;

            const data = this.#getSfxData();
            const cat = data.categories.find(c => c.id === categoryId);
            if (cat) {
              cat.sounds.push({ id: generateId('snd'), name, src, icon });
              await this.#setSfxData(data);
              this.render();
              ui.notifications.info(`Sound "${name}" added`);
            }
          }
        },
        cancel: { icon: '<i class="fas fa-times"></i>', label: 'Cancel' }
      },
      default: 'save'
    }).render(true);
  }

  #showEditSoundDialog(categoryId, soundId) {
    const data = this.#getSfxData();
    const cat = data.categories.find(c => c.id === categoryId);
    const sound = cat?.sounds?.find(s => s.id === soundId);
    if (!sound) return;

    new Dialog({
      title: `Edit: ${sound.name}`,
      content: `
        <form class="soundboard-dialog">
          <div class="form-group"><label>Name</label><input type="text" name="name" value="${sound.name}"></div>
          <div class="form-group"><label>URL</label><input type="text" name="src" value="${sound.src}"></div>
          <div class="form-group"><label>Icon</label><input type="text" name="icon" value="${sound.icon || 'fas fa-music'}"></div>
        </form>
      `,
      buttons: {
        save: {
          icon: '<i class="fas fa-save"></i>',
          label: 'Save',
          callback: async (html) => {
            sound.name = html.find('[name="name"]').val().trim() || sound.name;
            sound.src = html.find('[name="src"]').val().trim() || sound.src;
            sound.icon = html.find('[name="icon"]').val().trim() || 'fas fa-music';
            await this.#setSfxData(data);
            this.render();
          }
        },
        cancel: { icon: '<i class="fas fa-times"></i>', label: 'Cancel' }
      },
      default: 'save'
    }).render(true);
  }

  async #removeSfxSound(categoryId, soundId) {
    const data = this.#getSfxData();
    const cat = data.categories.find(c => c.id === categoryId);
    if (cat) {
      cat.sounds = cat.sounds.filter(s => s.id !== soundId);
      await this.#setSfxData(data);
      this.render();
    }
  }

  #showAddCategoryDialog() {
    new Dialog({
      title: 'New Category',
      content: `
        <form class="soundboard-dialog">
          <div class="form-group"><label>Name</label><input type="text" name="name" placeholder="Category"></div>
          <div class="form-group"><label>Icon</label><input type="text" name="icon" value="fas fa-folder"></div>
        </form>
      `,
      buttons: {
        save: {
          icon: '<i class="fas fa-save"></i>',
          label: 'Create',
          callback: async (html) => {
            const name = html.find('[name="name"]').val().trim();
            const icon = html.find('[name="icon"]').val().trim() || 'fas fa-folder';
            if (!name) return;

            const data = this.#getSfxData();
            data.categories.push({ id: generateId('cat'), name, icon, sounds: [] });
            await this.#setSfxData(data);
            this.render();
            ui.notifications.info(`Category "${name}" created`);
          }
        },
        cancel: { icon: '<i class="fas fa-times"></i>', label: 'Cancel' }
      },
      default: 'save'
    }).render(true);
  }

  async #removeSfxCategory(categoryId) {
    const data = this.#getSfxData();
    data.categories = data.categories.filter(c => c.id !== categoryId);
    await this.#setSfxData(data);
    this.render();
  }

  // ---------------------------------------------------------------------------
  // Play Random
  // ---------------------------------------------------------------------------

  #playRandomFromCategory(categoryId) {
    const categories = this.#getCategoriesForTab(this.#activeTab);
    const cat = categories.find(c => c.id === categoryId);
    if (!cat?.sounds?.length) return;

    const sound = cat.sounds[Math.floor(Math.random() * cat.sounds.length)];
    this.#handlePlay(sound.id, sound.src, {
      name: sound.name, icon: sound.icon, categoryId
    });

    this.#expandedCategories.add(categoryId);
  }

  // ---------------------------------------------------------------------------
  // Ratings
  // ---------------------------------------------------------------------------

  #getRatings() {
    return game.settings.get(MODULE_ID, 'ratings');
  }

  async #rateSound(soundId, value) {
    const ratings = this.#getRatings();
    if (ratings[soundId] === value) {
      delete ratings[soundId];
    } else {
      ratings[soundId] = value;
    }
    await game.settings.set(MODULE_ID, 'ratings', ratings);
    this.render();
  }

  // ---------------------------------------------------------------------------
  // Favorites
  // ---------------------------------------------------------------------------

  #showFavoriteDialog(soundId, name, src, icon) {
    const favorites = this.#getFavorites();
    const emptySlot = favorites.findIndex(f => !f);

    const slotOptions = favorites.map((f, i) => {
      const label = f ? `Slot ${i + 1}: ${f.name} (replace)` : `Slot ${i + 1}: Empty`;
      return `<option value="${i}" ${i === emptySlot ? 'selected' : ''}>${label}</option>`;
    }).join('');

    new Dialog({
      title: 'Add to Favorites',
      content: `
        <form class="soundboard-dialog">
          <div class="form-group"><label>Slot</label><select name="slot">${slotOptions}</select></div>
          <div class="form-group"><label>Display Name</label><input type="text" name="displayName" value="${name}"></div>
          <div class="form-group"><label>Color</label><input type="color" name="color" value="#c0392b"></div>
        </form>
      `,
      buttons: {
        save: {
          icon: '<i class="fas fa-star"></i>',
          label: 'Add',
          callback: async (html) => {
            const slot = parseInt(html.find('[name="slot"]').val());
            const displayName = html.find('[name="displayName"]').val().trim() || name;
            const color = html.find('[name="color"]').val();

            const favs = this.#getFavorites();
            favs[slot] = {
              id: soundId,
              name: displayName,
              src,
              icon: icon || 'fas fa-music',
              color,
              behavior: this.#getTabBehavior(this.#activeTab)
            };
            await this.#setFavorites(favs);
            this.render();
            ui.notifications.info(`"${displayName}" added to favorites`);
          }
        },
        cancel: { icon: '<i class="fas fa-times"></i>', label: 'Cancel' }
      },
      default: 'save'
    }).render(true);
  }

  async #removeFavorite(slot) {
    const favs = this.#getFavorites();
    favs[slot] = null;
    await this.#setFavorites(favs);
    this.render();
  }

  // ---------------------------------------------------------------------------
  // Public
  // ---------------------------------------------------------------------------

  async refresh() {
    this.#manifest = null;
    this.render();
  }
}

// ============================================================================
// Handlebars Helpers
// ============================================================================

Handlebars.registerHelper('eq', function(a, b) {
  return a === b;
});

Handlebars.registerHelper('not', function(a) {
  return !a;
});

// ============================================================================
// Socket Handling (All Clients)
// Uses native Audio elements to avoid CORS issues
// ============================================================================

const _socketSounds = new Map(); // src -> HTMLAudioElement

async function handleSocketMessage(data) {
  if (!data?.action) return;
  const volume = sliderToVolume(game.settings.get(MODULE_ID, 'globalVolume'));

  if (data.action === 'play') {
    try {
      const cachedUrl = await getCachedAudioUrl(data.src);
      const audio = new Audio(cachedUrl || data.src);
      audio.volume = data.volume ?? volume;
      audio.loop = data.loop ?? false;
      await audio.play();
      if (data.loop) _socketSounds.set(data.src, audio);
      if (!cachedUrl) cacheAudioInBackground(data.src);
    } catch (err) {
      console.warn(`${MODULE_ID} | Socket play failed:`, err);
    }
  } else if (data.action === 'stop') {
    const audio = _socketSounds.get(data.src);
    if (audio) {
      try { audio.pause(); audio.currentTime = 0; } catch (e) { /* */ }
      _socketSounds.delete(data.src);
    }
  } else if (data.action === 'pause') {
    const audio = _socketSounds.get(data.src);
    if (audio) try { audio.pause(); } catch (e) { /* */ }
  } else if (data.action === 'resume') {
    const audio = _socketSounds.get(data.src);
    if (audio) try { audio.play(); } catch (e) { /* */ }
  } else if (data.action === 'volume') {
    const audio = _socketSounds.get(data.src);
    if (audio) audio.volume = data.volume ?? 0.8;
  }
}

// ============================================================================
// Hooks
// ============================================================================

Hooks.once('init', () => {
  game.settings.register(MODULE_ID, 'sfxData', {
    scope: 'world',
    config: false,
    type: Object,
    default: {
      categories: [
        { id: 'melee-combat', name: 'Melee Combat', icon: 'fas fa-khanda', sounds: [] },
        { id: 'arcane-divine', name: 'Arcane & Divine', icon: 'fas fa-hat-wizard', sounds: [] },
        { id: 'environmental', name: 'Environmental', icon: 'fas fa-wind', sounds: [] },
        { id: 'monsters-npcs', name: 'Monsters & NPCs', icon: 'fas fa-dragon', sounds: [] }
      ]
    }
  });

  game.settings.register(MODULE_ID, 'favorites', {
    scope: 'world',
    config: false,
    type: Array,
    default: [null, null, null, null, null, null, null, null, null, null, null, null]
  });

  game.settings.register(MODULE_ID, 'ratings', {
    scope: 'world',
    config: false,
    type: Object,
    default: {}
  });

  game.settings.register(MODULE_ID, 'globalVolume', {
    name: 'Soundboard Volume',
    hint: 'Master volume for all soundboard sounds',
    scope: 'client',
    config: true,
    type: Number,
    range: { min: 0, max: 1, step: 0.05 },
    default: 0.5
  });

  game.settings.register(MODULE_ID, 'voicemodApiKey', {
    name: 'VoiceMod API Key',
    hint: 'Your VoiceMod Control API key. Get one at voicemod.net/developers',
    scope: 'client',
    config: true,
    type: String,
    default: ''
  });

  console.log(`${MODULE_ID} | Settings registered`);
});

Hooks.once('ready', () => {
  game.socket.on(`module.${MODULE_ID}`, handleSocketMessage);
  console.log(`${MODULE_ID} | Socket listener registered`);
});

Hooks.on('getSceneControlButtons', (controls) => {
  const tokenControls = controls.tokens;
  if (tokenControls?.tools) {
    tokenControls.tools[MODULE_ID] = {
      name: MODULE_ID,
      title: 'Soundboard',
      icon: 'fas fa-volume-up',
      button: true,
      visible: game.user.isGM,
      onChange: () => {
        if (SoundboardApp.instance?.rendered) {
          SoundboardApp.instance.close();
        } else {
          new SoundboardApp().render(true);
        }
      }
    };
  }
});

window.PF2eSoundboard = {
  open: () => new SoundboardApp().render(true),
  refresh: () => SoundboardApp.instance?.refresh()
};
