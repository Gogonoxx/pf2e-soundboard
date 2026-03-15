/**
 * Sound Manifest Generator
 *
 * Scans the local music folder and generates sound-manifest.json
 * for the PF2E Soundboard module.
 *
 * Usage: node generate-manifest.js
 *
 * Input:  Local music folder (MUSIC_BASE_PATH)
 * Output: ../data/sound-manifest.json
 */

const fs = require('fs');
const path = require('path');

// ============================================================================
// Configuration
// ============================================================================

const MUSIC_BASE_PATH = 'C:\\Users\\joshu\\OneDrive\\Dokumente\\Pathfinder 2E\\Musik';
const OUTPUT_PATH = path.join(__dirname, '..', 'data', 'sound-manifest.json');

// Worker proxy URL (same as map-browser)
const WORKER_BASE_URL = 'https://map-proxy.joshua-e6f.workers.dev';

// Base path on OneDrive (relative to the share root)
// This needs to match how the Worker resolves paths
const ONEDRIVE_BASE_PATH = '';

// Tab mapping: which top-level folders go to which tab
const TAB_MAPPING = {
  'Ambiente': 'ambience',
  'Mood': 'mood',
  // Everything else → 'themes'
};

// Folders to skip
const SKIP_FOLDERS = ['_ALT'];

// Audio file extensions
const AUDIO_EXTENSIONS = new Set(['.mp3', '.m4a', '.ogg', '.wav', '.flac', '.opus', '.webm', '.aac']);

// ============================================================================
// Manifest Generation
// ============================================================================

function generateId(prefix, name) {
  const slug = name
    .toLowerCase()
    .replace(/[äöüß]/g, m => ({ 'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss' }[m]))
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
  return `${prefix}-${slug}`;
}

function cleanSoundName(filename) {
  // Remove file extension
  let name = path.parse(filename).name;

  // Remove common prefixes like track numbers "4  Sudden Trouble - Cliff Martinez"
  name = name.replace(/^\d+\s+/, '');

  // Remove trailing metadata like "(128kbit_AAC)" or "[AudioTrimmer.com]"
  name = name.replace(/\s*\([^)]*\)\s*$/g, '');
  name = name.replace(/\s*\[[^\]]*\]\s*$/g, '');

  // Trim
  name = name.trim();

  return name || filename;
}

function scanFolder(folderPath) {
  const entries = [];

  if (!fs.existsSync(folderPath)) return entries;

  const items = fs.readdirSync(folderPath, { withFileTypes: true });

  for (const item of items) {
    if (item.isFile()) {
      const ext = path.extname(item.name).toLowerCase();
      if (AUDIO_EXTENSIONS.has(ext)) {
        entries.push(item.name);
      }
    }
  }

  return entries.sort();
}

function scanMusicFolder() {
  const manifest = {
    worker_base_url: WORKER_BASE_URL,
    onedrive_base_path: ONEDRIVE_BASE_PATH,
    generated_at: new Date().toISOString(),
    tabs: {
      ambience: { behavior: 'loop-multi', categories: [] },
      mood: { behavior: 'loop-single', categories: [] },
      themes: { behavior: 'loop-single', categories: [] }
    },
    stats: { total_files: 0, total_categories: 0 }
  };

  const topFolders = fs.readdirSync(MUSIC_BASE_PATH, { withFileTypes: true })
    .filter(d => d.isDirectory() && !SKIP_FOLDERS.includes(d.name))
    .map(d => d.name)
    .sort();

  for (const topFolder of topFolders) {
    const topPath = path.join(MUSIC_BASE_PATH, topFolder);
    const tab = TAB_MAPPING[topFolder] || 'themes';

    // Check if this folder has subfolders or just files
    const subItems = fs.readdirSync(topPath, { withFileTypes: true });
    const subFolders = subItems.filter(d => d.isDirectory()).map(d => d.name).sort();
    const directFiles = scanFolder(topPath);

    if (subFolders.length > 0) {
      // Has subfolders → each subfolder becomes a category
      for (const subFolder of subFolders) {
        const subPath = path.join(topPath, subFolder);
        const files = scanFolder(subPath);

        if (files.length === 0) continue;

        // For themes tab, prefix category name with top folder
        const categoryName = tab === 'themes'
          ? `${topFolder} — ${subFolder}`
          : subFolder;

        const category = {
          id: generateId('cat', `${topFolder}-${subFolder}`),
          name: categoryName,
          parent: topFolder,
          subfolder: subFolder,
          icon: guessIcon(topFolder, subFolder),
          sounds: files.map(file => ({
            id: generateId('snd', `${subFolder}-${path.parse(file).name}`).substring(0, 60),
            name: cleanSoundName(file),
            path: `${topFolder}/${subFolder}/${file}`,
            ext: path.extname(file).toLowerCase()
          }))
        };

        manifest.tabs[tab].categories.push(category);
        manifest.stats.total_files += files.length;
        manifest.stats.total_categories++;
      }
    }

    if (directFiles.length > 0) {
      // Direct files in top folder (no subfolder) → one category
      const category = {
        id: generateId('cat', topFolder),
        name: topFolder,
        parent: topFolder,
        subfolder: null,
        icon: guessIcon(topFolder, null),
        sounds: directFiles.map(file => ({
          id: generateId('snd', `${topFolder}-${path.parse(file).name}`).substring(0, 60),
          name: cleanSoundName(file),
          path: `${topFolder}/${file}`,
          ext: path.extname(file).toLowerCase()
        }))
      };

      manifest.tabs[tab].categories.push(category);
      manifest.stats.total_files += directFiles.length;
      manifest.stats.total_categories++;
    }
  }

  return manifest;
}

// ============================================================================
// Icon Guessing
// ============================================================================

function guessIcon(topFolder, subFolder) {
  const iconMap = {
    // Top-level
    'Ambiente': 'fas fa-wind',
    'Atmosphäre': 'fas fa-cloud-moon',
    'Ereignisse': 'fas fa-bolt',
    'Kampf': 'fas fa-crossed-swords',
    'Monster': 'fas fa-dragon',
    'Mood': 'fas fa-masks-theater',
    'Orfnir': 'fas fa-map',
    'Rassen': 'fas fa-users',
    'Flusswald': 'fas fa-tree',

    // Subfolder specifics
    'Taverne': 'fas fa-beer-mug-empty',
    'Wald': 'fas fa-tree',
    'Höhle': 'fas fa-mountain',
    'Dungeon': 'fas fa-dungeon',
    'Episch': 'fas fa-fire',
    'Stealth': 'fas fa-user-ninja',
    'Drachen': 'fas fa-dragon',
    'Untote': 'fas fa-skull',
    'Horror': 'fas fa-ghost',
    'Calm': 'fas fa-feather',
    'Creepy': 'fas fa-spider',
    'Magisch': 'fas fa-wand-sparkles',
    'Mysteriös': 'fas fa-eye',
    'Fey': 'fas fa-leaf',
    'Stadt': 'fas fa-city',
    'Ocean': 'fas fa-water',
    'Sturm': 'fas fa-cloud-bolt',
    'Schiff': 'fas fa-ship',
    'Lagerfeuer': 'fas fa-fire',
    'Markt': 'fas fa-store',
    'Elfen': 'fas fa-leaf',
    'Zwerge': 'fas fa-hammer',
    'Vampire': 'fas fa-teeth',
    'Duell': 'fas fa-khanda',
    'Assassins': 'fas fa-user-ninja',
    'Infiltration - Versteckt': 'fas fa-mask',
    'Infiltration - Alarm': 'fas fa-bell',
    'Barden': 'fas fa-guitar',
    'Verfolgung': 'fas fa-person-running',
  };

  // Try subfolder first, then top folder
  return iconMap[subFolder] || iconMap[topFolder] || 'fas fa-music';
}

// ============================================================================
// Main
// ============================================================================

// Ensure output directory exists
const outputDir = path.dirname(OUTPUT_PATH);
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

console.log(`Scanning: ${MUSIC_BASE_PATH}`);
const manifest = scanMusicFolder();

fs.writeFileSync(OUTPUT_PATH, JSON.stringify(manifest, null, 2), 'utf-8');

console.log(`\nManifest generated: ${OUTPUT_PATH}`);
console.log(`  Total files:      ${manifest.stats.total_files}`);
console.log(`  Total categories: ${manifest.stats.total_categories}`);
console.log(`  Ambience:         ${manifest.tabs.ambience.categories.length} categories`);
console.log(`  Mood:             ${manifest.tabs.mood.categories.length} categories`);
console.log(`  Themes:           ${manifest.tabs.themes.categories.length} categories`);
