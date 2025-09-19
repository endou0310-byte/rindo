// 県ごとの収集レベルを表示、A/B/Cは色、Dはグレー
const state = {
  favorites: JSON.parse(localStorage.getItem("favorites") || "[]"),
  sources: []
};

async function init() {
  // sources.json 読み込み
  const res = await fetch("./sources.json");
  state.sources = await res.json();

  // 県セレクト生成
  const prefSelect = document.getElementById("prefSelect");
  state.sources.forEach(src => {
    const opt = document.createElement("option");
    opt.value = src.pref;
    opt.textContent = `${src.name}`;
    opt.dataset.level = src.level;
    prefSelect.appendChild(opt);
  });

  // 地図
  const map = L.map('map').setView([35.68, 139.76], 7); // 関東域
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18, attribution: '&copy; OpenStreetMap'
  }).addTo(map);

  // 県ポップアップ（レベルバッジ + グレーアウト説明）
  prefSelect.addEventListener("change", () => showPrefBadge());
  showPrefBadge();

  // お気に入り
  document.getElementById("favBtn").addEventListener("click", addFavorite);
  renderFavorites();
}

function showPrefBadge() {
  const prefSelect = document.getElementById("prefSelect");
  const level = prefSelect.selectedOptions[0].dataset.level;
  const name = prefSelect.selectedOptions[0].textContent;

  // 画面上部に簡易バッジ表示（ヘッダタイトル横に追加するだけ）
  const header = document.querySelector("header");
  header.querySelectorAll(".badge").forEach(el => el.remove());
  const badge = document.createElement("span");
  badge.className = `badge ${level}`;
  badge.textContent = ` ${name}：${level} `;
  header.appendChild(badge);
}

function addFavorite() {
  const route = document.getElementById("routeInput").value.trim();
  const prefOpt = document.getElementById("prefSelect").selectedOptions[0];
  if (!route) return alert("路線名を入力してください。");
  const item = {
    id: Date.now(),
    route,
    pref: prefOpt.value,
    prefName: prefOpt.textContent,
    level: prefOpt.dataset.level
  };
  state.favorites.unshift(item);
  localStorage.setItem("favorites", JSON.stringify(state.favorites));
  document.getElementById("routeInput").value = "";
  renderFavorites();
}

function renderFavorites() {
  const ul = document.getElementById("favList");
  ul.innerHTML = "";
  state.favorites.forEach(f => {
    const li = document.createElement("li");
    li.innerHTML = `
      <strong>${f.route}</strong> — ${f.prefName}
      <span class="badge ${f.level}">${f.level}</span>
      <button data-id="${f.id}" class="rm">削除</button>
    `;
    ul.appendChild(li);
  });
  ul.querySelectorAll(".rm").forEach(btn => {
    btn.addEventListener("click", (e) => {
      const id = Number(e.target.dataset.id);
      state.favorites = state.favorites.filter(x => x.id !== id);
      localStorage.setItem("favorites", JSON.stringify(state.favorites));
      renderFavorites();
    });
  });
}

init();
