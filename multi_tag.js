(function () {
  'use strict';

  // ============ CONFIG ============
  const SHOW_DV_PROFILE = false;            // DV P7/P8.x или просто DV
  const COLORIZE_RATING = true;            // цветовая индикация рейтинга
  const RATING_COLOR_TEXT_ONLY = false;     // если true: красим текст, фон #f0f0f0

  // План и прогресс сезонов
  const ENABLE_PLANNED_EPISODES = true;     // подтягивать плановое число серий сезона
  const SHOW_SEASON_PROGRESS_BADGE = true;  // "Ep current/planned", если есть оба числа

  // TMDb (источник планов и статуса сериала)
  const ENABLE_TMDB = true;                 // TMDb для планов по сезонам
  const ENABLE_TMDB_ENDED = true;           // TMDb как источник статуса "Ended" для сериалов
  const TMDB_API_KEY = 'API_KEY';                  // <<< вставьте свой TMDb API key
  const TMDB_LANGUAGE = 'en-US';

  // Сериалы: бейдж Ended
  const SHOW_SERIES_ENDED_BADGE = true;     // показывать красный бейдж "Ended" (только если TMDb сказал)

  // Приоритет картинок / задержки
  const FETCH_DELAY_BEFORE_IMAGE_MS = 150;
  const FETCH_DELAY_ON_HOVER_MS      = 80;
  const FALLBACK_FETCH_TIMEOUT_MS    = 1200;

  // Форс-проходы (ранние волны)
  const FORCE_PASSES_MS = [80, 300, 900, 1800];

  // Ограничения на запросы
  const MAX_CONCURRENT_REQUESTS = 6;
  const PROCESS_TICK_MS         = 100;

  // Селекторы/визуал
  const VIEW_MARGIN = 200;
  const overlayClass = 'quality-overlay-label';
  const wrapperClass = 'quality-overlay-label-wrapper';

  // Бэйджи ставим ТОЛЬКО на контейнер изображения
  const TARGET_SELECTORS = [
    'a.cardImageContainer',   // сетка
    '.listItemImage'          // списки с фоном
  ].join(',');

  // ===== Служебные структуры =====
  const requestQueue = [];
  // cache: { [itemId]: { quality, rating, seasonPlanned?, seasonCurrent?, seriesEnded? (true|false) } }
  const overlayCache = {};
  const inflight = new Set();
  const waiters = new Map();
  const delayedTimers = new Map();
  const listenedImgs = new WeakSet();
  let activeRequests = 0;

  let intersectionObserver = null;
  const observedElements = new WeakSet();

  // ===== Внешние кэши =====
  // TVMaze
  const tvmazeShowIdCache = new Map();       // "tvdb:12345"/"imdb:tt..." -> showId
  const tvmazeSeasonOrderCache = new Map();  // `${showId}:${seasonNumber}` -> episodeOrder
  // TMDb
  const tmdbTvIdCache = new Map();             // 'tmdb:123'/'tvdb:123'/'imdb:tt...' -> tmdbId
  const tmdbSeasonsSnapshotCache = new Map();  // tmdbId -> seasons[]
  const tmdbSeasonCountCache = new Map();      // `${tmdbId}:${seasonNumber}` -> episode_count
  const tmdbTvStatusCache = new Map();         // tmdbId -> 'Ended' | 'Returning Series' | 'Canceled' ...

  // ===== ApiClient bootstrap =====
  const ApiClientRef =
    (typeof window !== 'undefined' && (window.ApiClient || (window.unsafeWindow && window.unsafeWindow.ApiClient)))
      ? (window.ApiClient || window.unsafeWindow.ApiClient)
      : null;

  if (!ApiClientRef) {
    const MAX_WAIT_MS = 15000;
    const start = Date.now();
    const timer = setInterval(() => {
      const api = window.ApiClient || (window.unsafeWindow && window.unsafeWindow.ApiClient);
      if (api || (Date.now() - start) > MAX_WAIT_MS) {
        clearInterval(timer);
        if (api) bootstrap(api);
      }
    }, 300);
  } else {
    bootstrap(ApiClientRef);
  }

  function bootstrap(ApiClient) {
    function getUserId() {
      try { return (ApiClient && ApiClient._serverInfo && ApiClient._serverInfo.UserId) || null; }
      catch { return null; }
    }

    // ===== Палитры рейтинга =====
    function ratingBgPalette(r) {
      const val = Number(r) || 0;
      if (val < 4) return '#c62828';
      if (val < 6) return '#ef6c00';
      if (val < 7) return '#f9a825';
      if (val < 8) return '#7cb342';
      return '#2e7d32';
    }
    function ratingTextPalette(r) {
      const val = Number(r) || 0;
      if (val < 4) return '#c62828';
      if (val < 6) return '#ef6c00';
      if (val < 7) return '#f9a825';
      if (val < 8) return '#7cb342';
      return '#2e7d32';
    }
    function textColorForBackground(hex) {
      const rgb = hexToRgb(hex || '#222');
      const L = 0.2126 * ch(rgb.r) + 0.7152 * ch(rgb.g) + 0.0722 * ch(rgb.b);
      return L > 0.6 ? '#111' : '#fff';
      function ch(v){ v/=255; return v<=0.03928? v/12.92 : Math.pow((v+0.055)/1.055,2.4); }
    }
    function hexToRgb(hex) {
      const h = hex.replace('#','');
      const full = h.length===3 ? h.split('').map(c=>c+c).join('') : h;
      const n = parseInt(full,16);
      return { r:(n>>16)&255, g:(n>>8)&255, b:n&255 };
    }

    function createLabel(label, type='quality', customBg=null, customTextColor=null) {
      const badge = document.createElement('div');
      badge.textContent = label;
      badge.className = overlayClass;

      let bgColor = customBg || '#444';
      let textColor = customTextColor || '#fff';

      if (!customBg) {
        if (type === 'quality') {
          switch (label) {
            case '4K': bgColor = '#0066cc'; break;
            case 'HD': bgColor = '#009900'; break;
            case 'SD': bgColor = '#666666'; break;
            case 'HDR': bgColor = '#cc0000'; break;
            default: break;
          }
          if (label === 'ATMOS') bgColor = '#ff6600';
          if (label.startsWith('DV')) bgColor = '#8000cc';
        } else if (type === 'rating') {
          bgColor = '#222';
        }
      }

      badge.style.cssText = `
        color: ${textColor};
        padding: 2px 6px;
        font-size: 11px;
        font-weight: bold;
        border-radius: 4px;
        pointer-events: none;
        user-select: none;
        white-space: nowrap;
        max-width: 100%;
        word-break: break-word;
        box-sizing: border-box;
        background-color: ${bgColor};
      `;
      return badge;
    }

    function addStyles() {
      const style = document.createElement('style');
      style.textContent = `
        .${overlayClass} {}
        .${wrapperClass} {
          position: absolute;
          top: 0; left: 0; right: 0;
          pointer-events: none;
          z-index: 120;
          display: flex;
          flex-wrap: wrap;
          justify-content: flex-start;
          align-items: flex-start;
          padding: 4px;
          gap: 4px;
          max-width: 100%;
        }
      `;
      document.head.appendChild(style);
    }

    // ===== DV helpers =====
    function hasDolbyVision(videoStream) {
      if (!videoStream) return false;
      const hasDvField =
        videoStream.DvProfile != null ||
        videoStream.DVProfile != null ||
        videoStream.dvProfile != null;
      const range = (videoStream.VideoRange || videoStream.VideoRangeType || '').toLowerCase();
      const hdrFormat = (videoStream.HDRFormat || '').toLowerCase();
      const saysDV = /(dolby.?vision|dovi)/.test(range) || /(dolby.?vision|dovi)/.test(hdrFormat);
      return hasDvField || saysDV;
    }
    function getDolbyVisionBadge(videoStream) {
      if (!videoStream) return null;
      const profile =
        videoStream.DvProfile ?? videoStream.DVProfile ?? videoStream.dvProfile ?? null;
      const blId =
        videoStream.DvBlSignalCompatibilityId ??
        videoStream.DVBlSignalCompatibilityId ??
        videoStream.dvBlSignalCompatibilityId ??
        null;
      const range = (videoStream.VideoRange || videoStream.VideoRangeType || '').toLowerCase();
      const hdrFormat = (videoStream.HDRFormat || '').toLowerCase();
      const saysDV = /dovi|dolby/.test(range) || /dovi|dolby/.test(hdrFormat);
      if (profile == null) return saysDV ? 'DV' : null;
      let p = Number(profile);
      if (!Number.isFinite(p)) {
        const m = String(profile).match(/(\d+)/); p = m ? Number(m[1]) : NaN;
      }
      if (!Number.isFinite(p)) return 'DV';
      if (p === 8 && blId != null && String(blId).trim() !== '') return `DV P8.${blId}`;
      return `DV P${p}`;
    }

    function getQuality(videoStream, audioStreams=[]) {
      if (!videoStream) return null;
      const height = videoStream.Height || 0;
      let quality = 'SD';
      if (height >= 1440) quality = '4K';
      else if (height >= 531) quality = 'HD';

      const range = (videoStream.VideoRange || videoStream.VideoRangeType || '').toLowerCase();
      const hdrFormat = (videoStream.HDRFormat || '').toLowerCase();
      const isHDR = /hdr|hlg|pq/.test(range) || /hdr|hlg|pq/.test(hdrFormat);

      const isDV = hasDolbyVision(videoStream);
      const dvLabel = SHOW_DV_PROFILE ? getDolbyVisionBadge(videoStream) : (isDV ? 'DV' : null);

      const hasAtmos = audioStreams.some(a =>
        /atmos/i.test(a.DisplayTitle || a.Title || '') ||
        /atmos/i.test(a.AudioCodec || '')
      );

      const parts = [quality];
      if (isHDR) parts.push('HDR');
      if (dvLabel) parts.push(dvLabel);
      if (hasAtmos) parts.push('ATMOS');
      return parts.join(' ');
    }

    // ===== ItemId helpers =====
    function extractItemIdFromBg(el) {
      const inline = el.getAttribute('style') || '';
      let m = inline.match(/\/Items\/([a-f0-9]{32})\/Images/i);
      if (m) return m[1];
      const bg = (getComputedStyle(el).backgroundImage || '');
      m = bg.match(/\/Items\/([a-f0-9]{32})\/Images/i);
      return m ? m[1] : null;
    }
    function extractItemId(el) {
      const withDataId = el.closest('[data-id]');
      if (withDataId?.dataset?.id && /^[a-f0-9]{32}$/i.test(withDataId.dataset.id)) {
        return withDataId.dataset.id;
      }
      const link = el.closest('a[href*="id="]') || el;
      if (link && link.href) {
        const m = link.href.match(/(?:\?|&)id=([a-f0-9]{32})/i);
        if (m) return m[1];
      }
      if (el.classList && el.classList.contains('listItemImage')) {
        const fromBg = extractItemIdFromBg(el);
        if (fromBg) return fromBg;
      }
      return null;
    }

    function allTargets() {
      return document.querySelectorAll(TARGET_SELECTORS);
    }
    function isInViewport(el, margin = VIEW_MARGIN) {
      const rect = el.getBoundingClientRect();
      return (
        rect.bottom >= -margin &&
        rect.right  >= -margin &&
        rect.top    <= (window.innerHeight || document.documentElement.clientHeight) + margin &&
        rect.left   <= (window.innerWidth  || document.documentElement.clientWidth)  + margin
      );
    }

    // ===== Сетевые утилиты =====
    async function fetchJSON(url, timeoutMs = 2500) {
      const ctrl = new AbortController();
      const id = setTimeout(() => ctrl.abort(), timeoutMs);
      try {
        const r = await fetch(url, { signal: ctrl.signal });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return await r.json();
      } finally { clearTimeout(id); }
    }

    // ===== TMDb helpers =====
    function tmdbUrl(path, params = {}) {
      const u = new URL(`https://api.themoviedb.org/3${path}`);
      u.searchParams.set('api_key', TMDB_API_KEY);
      if (TMDB_LANGUAGE) u.searchParams.set('language', TMDB_LANGUAGE);
      for (const [k, v] of Object.entries(params)) {
        if (v != null) u.searchParams.set(k, v);
      }
      return u.toString();
    }
    async function tmdbLookupTvIdByProvider(providerIds) {
      const rawTmdb = providerIds?.Tmdb ?? providerIds?.TMDb ?? providerIds?.tmdb;
      if (rawTmdb) {
        const key = 'tmdb:' + String(rawTmdb);
        const val = Number(rawTmdb);
        tmdbTvIdCache.set(key, val);
        return val;
      }
      if (providerIds?.Tvdb) {
        const key = 'tvdb:' + String(providerIds.Tvdb);
        if (tmdbTvIdCache.has(key)) return tmdbTvIdCache.get(key);
        try {
          const data = await fetchJSON(tmdbUrl(`/find/${encodeURIComponent(providerIds.Tvdb)}`, { external_source: 'tvdb_id' }), 3000);
          const id = data?.tv_results?.[0]?.id;
          if (id) { tmdbTvIdCache.set(key, id); return id; }
        } catch {}
      }
      if (providerIds?.Imdb) {
        const key = 'imdb:' + String(providerIds.Imdb);
        if (tmdbTvIdCache.has(key)) return tmdbTvIdCache.get(key);
        try {
          const data = await fetchJSON(tmdbUrl(`/find/${encodeURIComponent(providerIds.Imdb)}`, { external_source: 'imdb_id' }), 3000);
          const id = data?.tv_results?.[0]?.id;
          if (id) { tmdbTvIdCache.set(key, id); return id; }
        } catch {}
      }
      return null;
    }
    function isTmdbStatusEnded(status) {
      const s = String(status || '').toLowerCase();
      // TMDb: "Ended", "Returning Series", "Planned", "In Production", "Canceled", "Pilot"
      return s === 'ended' || s === 'canceled' || s === 'cancelled';
    }
    async function tmdbGetTvStatus(tmdbId) {
      if (tmdbTvStatusCache.has(tmdbId)) return tmdbTvStatusCache.get(tmdbId);
      try {
        const tv = await fetchJSON(tmdbUrl(`/tv/${tmdbId}`), 3500);
        if (Array.isArray(tv?.seasons)) tmdbSeasonsSnapshotCache.set(tmdbId, tv.seasons);
        if (tv?.status) tmdbTvStatusCache.set(tmdbId, tv.status);
        return tv?.status ?? null;
      } catch {
        tmdbTvStatusCache.set(tmdbId, null);
        return null;
      }
    }
    async function tmdbGetSeasonPlannedCount(tmdbId, seasonNumber) {
      const cacheKey = `${tmdbId}:${seasonNumber}`;
      if (tmdbSeasonCountCache.has(cacheKey)) return tmdbSeasonCountCache.get(cacheKey);

      let seasons = tmdbSeasonsSnapshotCache.get(tmdbId);
      if (!seasons) {
        try {
          const tv = await fetchJSON(tmdbUrl(`/tv/${tmdbId}`), 3500);
          seasons = Array.isArray(tv?.seasons) ? tv.seasons : null;
          if (seasons) tmdbSeasonsSnapshotCache.set(tmdbId, seasons);
          if (tv?.status) tmdbTvStatusCache.set(tmdbId, tv.status);
        } catch {
          seasons = null;
        }
      }
      if (seasons) {
        const hit = seasons.find(s => Number(s?.season_number) === Number(seasonNumber));
        if (hit && typeof hit.episode_count === 'number') {
          tmdbSeasonCountCache.set(cacheKey, hit.episode_count);
          return hit.episode_count;
        }
      }
      try {
        const season = await fetchJSON(tmdbUrl(`/tv/${tmdbId}/season/${seasonNumber}`), 3500);
        const count = Array.isArray(season?.episodes) ? season.episodes.length : null;
        tmdbSeasonCountCache.set(cacheKey, (typeof count === 'number' ? count : null));
        return count ?? null;
      } catch {
        tmdbSeasonCountCache.set(cacheKey, null);
        return null;
      }
    }

    // ===== TVMaze helpers (фолбэк для планов) =====
    async function tvmazeLookupShowIdByProvider(providerIds) {
      if (providerIds?.Tvdb) {
        const key = 'tvdb:' + String(providerIds.Tvdb);
        if (tvmazeShowIdCache.has(key)) return tvmazeShowIdCache.get(key);
        try {
          const data = await fetchJSON(`https://api.tvmaze.com/lookup/shows?thetvdb=${encodeURIComponent(providerIds.Tvdb)}`, 2500);
          if (data?.id) { tvmazeShowIdCache.set(key, data.id); return data.id; }
        } catch {}
      }
      if (providerIds?.Imdb) {
        const key = 'imdb:' + String(providerIds.Imdb);
        if (tvmazeShowIdCache.has(key)) return tvmazeShowIdCache.get(key);
        try {
          const data = await fetchJSON(`https://api.tvmaze.com/lookup/shows?imdb=${encodeURIComponent(providerIds.Imdb)}`, 2500);
          if (data?.id) { tvmazeShowIdCache.set(key, data.id); return data.id; }
        } catch {}
      }
      return null;
    }
    async function tvmazeGetSeasonEpisodeOrder(showId, seasonNumber) {
      const cacheKey = `${showId}:${seasonNumber}`;
      if (tvmazeSeasonOrderCache.has(cacheKey)) return tvmazeSeasonOrderCache.get(cacheKey);
      try {
        const seasons = await fetchJSON(`https://api.tvmaze.com/shows/${showId}/seasons`, 3000);
        const s = Array.isArray(seasons) ? seasons.find(x => Number(x.number) === Number(seasonNumber)) : null;
        const order = (s && typeof s.episodeOrder === 'number') ? s.episodeOrder : null;
        tvmazeSeasonOrderCache.set(cacheKey, order);
        return order;
      } catch {
        tvmazeSeasonOrderCache.set(cacheKey, null);
        return null;
      }
    }

    // ===== Current episodes in season (в библиотеке) =====
    async function fetchSeasonCurrentEpisodes(userId, seasonId, seasonItem) {
      if (typeof seasonItem?.ChildCount === 'number' && seasonItem.ChildCount > 0) {
        return seasonItem.ChildCount;
      }
      try {
        const resp = await ApiClient.ajax({
          type: 'GET',
          url: ApiClient.getUrl('/Items', {
            ParentId: seasonId,
            IncludeItemTypes: 'Episode',
            Recursive: false,
            Limit: 1,
            userId
          }),
          dataType: 'json'
        });
        if (typeof resp?.TotalRecordCount === 'number') return resp.TotalRecordCount;
      } catch {}
      return null;
    }

    // ===== Planned episodes aggregator =====
    async function fetchPlannedSeasonEpisodes(userId, seasonItem) {
      if (!ENABLE_PLANNED_EPISODES) return null;

      const seasonNumber = typeof seasonItem?.IndexNumber === 'number' ? seasonItem.IndexNumber : null;
      const seriesId = seasonItem?.SeriesId || seasonItem?.ParentId || null;
      if (seasonNumber == null || !seriesId) return null;

      try {
        const series = await ApiClient.getItem(userId, seriesId);
        const providerIds = series?.ProviderIds || {};

        // TMDb приоритетно
        if (ENABLE_TMDB && TMDB_API_KEY) {
          const tmdbId = await tmdbLookupTvIdByProvider(providerIds);
          if (tmdbId) {
            const planned = await tmdbGetSeasonPlannedCount(tmdbId, seasonNumber);
            if (typeof planned === 'number' && planned > 0) return planned;
          }
        }
        // TVMaze — фолбэк
        const showId = await tvmazeLookupShowIdByProvider(providerIds);
        if (showId) {
          const planned = await tvmazeGetSeasonEpisodeOrder(showId, seasonNumber);
          if (typeof planned === 'number' && planned > 0) return planned;
        }
      } catch {}
      return null;
    }

    // ===== Series ended via TMDb ONLY =====
    async function fetchSeriesEndedFromTMDb(seriesItem) {
      if (!ENABLE_TMDB_ENDED || !TMDB_API_KEY) return null;  // нет TMDb — нет бейджа
      try {
        const providerIds = seriesItem?.ProviderIds || {};
        const tmdbId = await tmdbLookupTvIdByProvider(providerIds);
        if (!tmdbId) return null;
        const status = await tmdbGetTvStatus(tmdbId);
        return status != null ? isTmdbStatusEnded(status) : null;
      } catch { return null; }
    }

    // ===== Compose Ep badge text =====
    function composeEpBadgeText(data) {
      const cur = typeof data.seasonCurrent === 'number' ? data.seasonCurrent : null;
      const plan = typeof data.seasonPlanned === 'number' ? data.seasonPlanned : null;

      if (SHOW_SEASON_PROGRESS_BADGE && cur != null && plan != null && plan > 0) {
        if (cur > plan) return `Ep ${cur}`;
        return `Ep ${cur}/${plan}`;
      }
      if (plan != null) return `Ep ${plan}`;
      if (cur != null)  return `Ep ${cur}`;
      return null;
    }

    // ===== Jellyfin helpers =====
    async function fetchFirstEpisode(userId, parentId) {
      try {
        const resp = await ApiClient.ajax({
          type: 'GET',
          url: ApiClient.getUrl('/Items', {
            ParentId: parentId,
            IncludeItemTypes: 'Episode',
            Recursive: true,
            SortBy: 'PremiereDate',
            SortOrder: 'Ascending',
            Limit: 1,
            userId
          }),
          dataType: 'json'
        });
        return resp.Items?.[0] || null;
      } catch { return null; }
    }

    // ===== API fetch per item =====
    async function fetchAndFill(itemId) {
      if (!itemId) return;
      if (overlayCache[itemId]) { deliverToWaiters(itemId, overlayCache[itemId], itemId); return; }
      if (inflight.has(itemId)) return;

      inflight.add(itemId);
      const userId = getUserId();
      if (!userId) { inflight.delete(itemId); return; }

      try {
        const item = await ApiClient.getItem(userId, itemId);

        let qualityString = null;
        if (item.Type === 'Series') {
          const ep = await fetchFirstEpisode(userId, itemId);
          if (ep?.Id) {
            const fullEp = await ApiClient.getItem(userId, ep.Id);
            const media = fullEp?.MediaSources?.[0];
            const v = media?.MediaStreams?.find(s => s.Type === 'Video');
            const a = media?.MediaStreams?.filter(s => s.Type === 'Audio') || [];
            if (v?.Height) qualityString = getQuality(v, a);
          }
        } else if (item.Type === 'Season') {
          const ep = await fetchFirstEpisode(userId, itemId);
          if (ep?.Id) {
            const fullEp = await ApiClient.getItem(userId, ep.Id);
            const media = fullEp?.MediaSources?.[0];
            const v = media?.MediaStreams?.find(s => s.Type === 'Video');
            const a = media?.MediaStreams?.filter(s => s.Type === 'Audio') || [];
            if (v?.Height) qualityString = getQuality(v, a);
          }
        } else {
          const media = item?.MediaSources?.[0];
          const v = media?.MediaStreams?.find(s => s.Type === 'Video');
          const a = media?.MediaStreams?.filter(s => s.Type === 'Audio') || [];
          if (v?.Height) qualityString = getQuality(v, a);
        }

        // Рейтинг
        let ratingValue = null;
        if (typeof item?.CommunityRating === 'number') ratingValue = item.CommunityRating;
        else if (typeof item?.CriticRating === 'number') ratingValue = item.CriticRating;

        // Сразу отдаём доступное
        const initialData = { quality: qualityString, rating: ratingValue };
        overlayCache[itemId] = initialData;
        deliverToWaiters(itemId, initialData, itemId);

        // СЕРИАЛ: статус Ended ТОЛЬКО из TMDb
        if (item.Type === 'Series' && SHOW_SERIES_ENDED_BADGE) {
          const ended = await fetchSeriesEndedFromTMDb(item); // true | false | null
          if (ended !== null) {
            overlayCache[itemId] = { ...overlayCache[itemId], seriesEnded: !!ended };
          } else {
            // нет данных TMDb — не показываем бейдж, убедимся что флажок отсутствует
            const { seriesEnded, ...rest } = overlayCache[itemId];
            overlayCache[itemId] = rest;
          }
          updateEndedBadgeForItem(itemId);
        }

        // СЕЗОН: план/текущие эпизоды
        if (item.Type === 'Season') {
          // current
          if (typeof item?.ChildCount === 'number' && item.ChildCount > 0) {
            overlayCache[itemId] = { ...overlayCache[itemId], seasonCurrent: item.ChildCount };
            updateOverlaysForItem(itemId);
          } else {
            const current = await fetchSeasonCurrentEpisodes(userId, item.Id, item);
            if (typeof current === 'number') {
              overlayCache[itemId] = { ...overlayCache[itemId], seasonCurrent: current };
              updateOverlaysForItem(itemId);
            }
          }
          // planned
          if (ENABLE_PLANNED_EPISODES) {
            const planned = await fetchPlannedSeasonEpisodes(userId, item);
            if (typeof planned === 'number' && planned > 0) {
              overlayCache[itemId] = { ...overlayCache[itemId], seasonPlanned: planned };
              updateOverlaysForItem(itemId);
            }
          }
        }
      } catch {
        // noop
      } finally {
        inflight.delete(itemId);
      }
    }

    function deliverToWaiters(itemId, data, idForWrapper) {
      const set = waiters.get(itemId);
      if (!set || set.size === 0) return;
      set.forEach(container => insertOverlay(container, data, idForWrapper));
      waiters.delete(itemId);
    }

    function insertOverlay(container, data, itemIdForWrapper) {
      if (!container || !container.matches(TARGET_SELECTORS)) return;
      if (container.querySelector(`.${wrapperClass}`)) return;

      const wrapper = document.createElement('div');
      wrapper.className = wrapperClass;
      if (itemIdForWrapper) wrapper.dataset.itemid = itemIdForWrapper;

      // СЕРИАЛ: бейдж Ended — только если TMDb сказал (seriesEnded === true)
      if (data.seriesEnded === true && SHOW_SERIES_ENDED_BADGE) {
        const endedBadge = createLabel('Ended', 'meta', '#c62828', '#ffffff');
        endedBadge.setAttribute('data-ended', '1');
        wrapper.appendChild(endedBadge);
      }

      // Качество
      if (data.quality) {
        data.quality.split(' ').forEach(label => {
          const badge = createLabel(label, 'quality');
          wrapper.appendChild(badge);
        });
      }

      // СЕЗОН: EP / прогресс
      const epText = composeEpBadgeText(data);
      if (epText) {
        const epBadge = createLabel(epText, 'meta');
        epBadge.setAttribute('data-ep', '1');
        wrapper.appendChild(epBadge);
      }

      // Рейтинг
      if (typeof data.rating === 'number') {
        const value = Math.round(data.rating * 10) / 10;
        let ratingBg = null, ratingText = null;
        if (COLORIZE_RATING) {
          if (RATING_COLOR_TEXT_ONLY) {
            ratingText = ratingTextPalette(value);
            ratingBg = '#f0f0f0';
          } else {
            ratingBg = ratingBgPalette(value);
            ratingText = textColorForBackground(ratingBg);
          }
        }
        const ratingBadge = createLabel(`★ ${value}`, 'rating', ratingBg, ratingText);
        wrapper.appendChild(ratingBadge);
      }

      if (getComputedStyle(container).position === 'static') {
        container.style.position = 'relative';
      }
      container.appendChild(wrapper);
    }

    // Обновить/добавить/убрать бейдж Ended при асинхронном ответе TMDb
    function updateEndedBadgeForItem(itemId) {
      const data = overlayCache[itemId];
      const wrappers = document.querySelectorAll(`.${wrapperClass}[data-itemid="${itemId}"]`);
      wrappers.forEach(w => {
        let badge = w.querySelector('.' + overlayClass + '[data-ended="1"]');
        if (data && data.seriesEnded === true && SHOW_SERIES_ENDED_BADGE) {
          if (!badge) {
            badge = createLabel('Ended', 'meta', '#c62828', '#ffffff');
            badge.setAttribute('data-ended', '1');
            w.insertBefore(badge, w.firstChild);
          }
        } else {
          if (badge) badge.remove();
        }
      });
    }

    // Обновить/добавить EP-бэйдж, если данные пришли позже
    function updateOverlaysForItem(itemId) {
      const data = overlayCache[itemId];
      if (!data) return;
      const wrappers = document.querySelectorAll(`.${wrapperClass}[data-itemid="${itemId}"]`);
      wrappers.forEach(w => {
        const txt = composeEpBadgeText(data);
        if (!txt) return;
        let ep = w.querySelector('.' + overlayClass + '[data-ep="1"]');
        if (!ep) {
          ep = createLabel(txt, 'meta');
          ep.setAttribute('data-ep', '1');
          w.appendChild(ep);
        } else {
          ep.textContent = txt;
        }
      });
    }

    // ===== Очередь =====
    function scheduleFetch(itemId, delayMs) {
      if (!itemId) return;
      if (delayedTimers.has(itemId)) return;
      const id = setTimeout(() => {
        delayedTimers.delete(itemId);
        enqueueItem(itemId);
      }, Math.max(0, delayMs|0));
      const hardId = setTimeout(() => {
        const rec = delayedTimers.get(itemId);
        if (rec && rec.id === id) {
          delayedTimers.delete(itemId);
          enqueueItem(itemId);
        }
      }, FALLBACK_FETCH_TIMEOUT_MS);
      delayedTimers.set(itemId, { id, hardId, cancel(){ clearTimeout(id); clearTimeout(hardId); } });
    }
    function cancelScheduled(itemId) {
      const rec = delayedTimers.get(itemId);
      if (rec && rec.cancel) rec.cancel();
      delayedTimers.delete(itemId);
    }
    function enqueueItem(itemId) {
      if (!itemId) return;
      if (overlayCache[itemId] || inflight.has(itemId)) return;
      if (requestQueue.includes(itemId)) return;
      requestQueue.push(itemId);
    }
    function processQueue() {
      if (activeRequests >= MAX_CONCURRENT_REQUESTS || requestQueue.length === 0) return;
      const itemId = requestQueue.shift();
      activeRequests++;
      Promise.resolve()
        .then(() => fetchAndFill(itemId))
        .finally(() => { activeRequests--; });
    }
    setInterval(processQueue, PROCESS_TICK_MS);

    // ===== Готовность изображения =====
    function isImageReady(el) {
      if (el.classList.contains('listItemImage')) {
        const bg = (getComputedStyle(el).backgroundImage || '');
        const hasBg = /\burl\(/i.test(bg);
        const notLazy = !el.classList.contains('lazy');
        return hasBg || notLazy;
      } else {
        const img = el.querySelector('img');
        return !!(img && img.complete && img.naturalWidth > 0);
      }
    }
    function attachImgLoadListener(el) {
      if (!el.matches('a.cardImageContainer')) return;
      const img = el.querySelector('img');
      if (!img) return;
      if (listenedImgs.has(img)) return;
      listenedImgs.add(img);

      if (img.complete && img.naturalWidth > 0) {
        const itemId = extractItemId(el);
        if (itemId) {
          registerWaiter(itemId, el);
          cancelScheduled(itemId);
          scheduleFetch(itemId, 0);
        }
        return;
      }
      img.addEventListener('load', () => {
        const itemId = extractItemId(el);
        if (!itemId) return;
        registerWaiter(itemId, el);
        cancelScheduled(itemId);
        scheduleFetch(itemId, 0);
      }, { once: true });
    }
    const bgObserver = new MutationObserver(mutations => {
      for (const m of mutations) {
        const el = m.target;
        if (!(el instanceof Element)) continue;
        if (!el.classList.contains('listItemImage')) continue;

        if (m.attributeName === 'class' || m.attributeName === 'style') {
          if (isImageReady(el)) {
            const itemId = extractItemId(el);
            if (!itemId) continue;
            registerWaiter(itemId, el);
            cancelScheduled(itemId);
            scheduleFetch(itemId, 0);
          }
        }
      }
    });
    function observeImageReadiness(el) {
      if (el.matches('.listItemImage')) {
        bgObserver.observe(el, { attributes: true, attributeFilter: ['class', 'style'] });
      } else if (el.matches('a.cardImageContainer')) {
        attachImgLoadListener(el);
      }
    }

    // ===== Наблюдатели, ховеры и форсы =====
    function observeIntersections() {
      if (intersectionObserver) intersectionObserver.disconnect();
      intersectionObserver = new IntersectionObserver(entries => {
        for (const entry of entries) {
          const el = entry.target;
          if (!entry.isIntersecting) continue;
          if (observedElements.has(el)) continue;

          observedElements.add(el);
          intersectionObserver.unobserve(el);

          const itemId = extractItemId(el);
          if (!itemId) continue;

          registerWaiter(itemId, el);

          if (isImageReady(el)) {
            cancelScheduled(itemId);
            scheduleFetch(itemId, 0);
          } else {
            scheduleFetch(itemId, FETCH_DELAY_BEFORE_IMAGE_MS);
          }

          observeImageReadiness(el);
        }
      }, { rootMargin: VIEW_MARGIN + 'px' });
      scanTargets();
    }

    function registerWaiter(itemId, container) {
      let set = waiters.get(itemId);
      if (!set) { set = new Set(); waiters.set(itemId, set); }
      set.add(container);
      if (overlayCache[itemId]) {
        deliverToWaiters(itemId, overlayCache[itemId], itemId);
      }
    }

    function scanTargets() {
      allTargets().forEach(el => {
        if (!observedElements.has(el)) {
          intersectionObserver.observe(el);
        }
        observeImageReadiness(el);
      });
    }

    document.addEventListener('mouseenter', (e) => {
      const el = e.target.closest(TARGET_SELECTORS);
      if (!el) return;
      const itemId = extractItemId(el);
      if (!itemId) return;
      registerWaiter(itemId, el);
      cancelScheduled(itemId);
      scheduleFetch(itemId, isImageReady(el) ? 0 : FETCH_DELAY_ON_HOVER_MS);
    }, true);

    function forceFetchVisibleMissing() {
      allTargets().forEach(el => {
        if (!isInViewport(el)) return;
        const itemId = extractItemId(el);
        if (!itemId) return;
        registerWaiter(itemId, el);
        cancelScheduled(itemId);
        scheduleFetch(itemId, isImageReady(el) ? 0 : FETCH_DELAY_BEFORE_IMAGE_MS);
      });
    }

    function cleanupWrongPlacements() {
      document.querySelectorAll(`.${wrapperClass}`).forEach(w => {
        const p = w.parentElement;
        if (!p) return;
        if (!p.matches(TARGET_SELECTORS)) w.remove();
      });
    }

    let forcePassTimerIds = [];
    function scheduleForcePasses() {
      forcePassTimerIds.forEach(id => clearTimeout(id));
      forcePassTimerIds = [];

      cleanupWrongPlacements();

      FORCE_PASSES_MS.forEach(ms => {
        const id = setTimeout(() => {
          scanTargets();
          forceFetchVisibleMissing();
          cleanupWrongPlacements();
        }, ms);
        forcePassTimerIds.push(id);
      });
    }

    // MutationObserver (DOM изменения)
    let mutationTimeout;
    const mutationObserver = new MutationObserver(() => {
      clearTimeout(mutationTimeout);
      mutationTimeout = setTimeout(() => {
        scanTargets();
        scheduleForcePasses();
      }, 150);
    });

    // SPA URL changes
    (function hookHistory() {
      const _push = history.pushState;
      const _replace = history.replaceState;
      function onUrlChange() {
        observedElements.clear?.();
        observeIntersections();
        scheduleForcePasses();
      }
      history.pushState = function () { const r = _push.apply(this, arguments); onUrlChange(); return r; };
      history.replaceState = function () { const r = _replace.apply(this, arguments); onUrlChange(); return r; };
      window.addEventListener('popstate', onUrlChange, { passive: true });
    })();

    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') {
        scanTargets();
        scheduleForcePasses();
      }
    });

    addStyles();
    observeIntersections();
    mutationObserver.observe(document.body, { childList: true, subtree: true });
    scanTargets();
    scheduleForcePasses();
  }
})();
