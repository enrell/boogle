import requests
from bs4 import BeautifulSoup
import json
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www.gutenberg.org"

# ---------------- Salvar aqui. É o melhor jeito? não. Vou arrumar isso? SDS (Só Deus Sabe) ----------------
class BancoDeDados:
    def __init__(self, nome_banco="livros.db", schema_file="schema.sql"):
        self.conn = sqlite3.connect(nome_banco)
        self.carregar_schema(schema_file)

    def carregar_schema(self, schema_file):
        with open(schema_file, "r", encoding="utf-8") as f:
            schema = f.read()
        cursor = self.conn.cursor()
        cursor.executescript(schema)
        self.conn.commit()

    def salvar_livro(self, livro):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO livros (id, title, author, downloads, cover, language, 
                                    reading_level_score, reading_level_text, subjects, summary, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                livro["id"], livro["title"], livro["author"], livro["downloads"],
                livro["cover"], livro["language"], livro["reading_level_score"],
                livro["reading_level_text"], ", ".join(livro["subjects"]), livro["summary"], livro["url"]
            ))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass


# ---------------- Apenas para colocar um texto mais facil no ranqueamento dos livros (se é mais facil de ler ou não) ----------------
def simplificar_reading_level(texto):
    if not texto:
        return None, None
    m_num = re.search(r"(\d+(\.\d+)?)", texto)
    nota = float(m_num.group(1)) if m_num else None
    m_interp = re.search(r"\.\s*([^\.]+)\.?$", texto)
    interpretacao = m_interp.group(1).strip() if m_interp else None
    return nota, interpretacao


# ---------------- Aqui vamos roubar os livros de fato, quis pegar de um arquivo onde ja limpei os links pra ficar mais facil ----------------
class GutenbergScraper:
    def __init__(self, categoria_url):
        self.categoria_url = categoria_url

    def extrair_id_livro(self, href):
        m = re.match(r"^/ebooks/(\d+)$", href)
        return m.group(1) if m else None

    def parse_downloads(self, text):
        m = re.search(r"(\d+)", text or "")
        return int(m.group(1)) if m else None

    def coletar_livros_categoria(self, paginas=1, limite_livros=None):
        livros = []
        url_atual = self.categoria_url
        coletados = 0

        for _ in range(paginas):
            r = requests.get(url_atual)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "html.parser")

            for li in soup.find_all("li", class_="booklink"):
                a = li.find("a", class_="link")
                if not a or not a.get("href"):
                    continue

                href = a["href"].strip()
                livro_id = self.extrair_id_livro(href)
                if not livro_id:
                    continue

                titulo = a.find("span", class_="title").get_text(strip=True)
                autor = a.find("span", class_="subtitle").get_text(strip=True) if a.find("span", class_="subtitle") else "N/A"
                downloads_raw = a.find("span", class_="extra").get_text(strip=True) if a.find("span", class_="extra") else None
                downloads = self.parse_downloads(downloads_raw)
                capa_el = a.find("img", class_="cover-thumb")
                capa_url = BASE_URL + capa_el["src"] if capa_el else None
                url_livro = BASE_URL + href

                detalhes = self.coletar_detalhes_livro(url_livro)
                nota, interpretacao = simplificar_reading_level(detalhes.get("reading_level"))

                livro = {
                    "id": livro_id,
                    "title": titulo,
                    "author": autor,
                    "downloads": downloads,
                    "cover": capa_url,
                    "language": detalhes.get("language"),
                    "reading_level_score": nota,
                    "reading_level_text": interpretacao,
                    "subjects": detalhes.get("subjects"),
                    "summary": detalhes.get("summary"),
                    "url": url_livro
                }
                livros.append(livro)
                coletados += 1

                if limite_livros and coletados >= limite_livros:
                    return livros

            next_a = soup.find("a", title="Go to the next page of results.")
            if next_a:
                url_atual = BASE_URL + next_a["href"]
            else:
                break

        return livros

    def coletar_detalhes_livro(self, url_livro):
        r = requests.get(url_livro)
        if r.status_code != 200:
            return {}
        soup = BeautifulSoup(r.text, "html.parser")
        detalhes = {"language": None, "reading_level": None, "subjects": [], "summary": None}

        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True).lower()
            value = td.get_text(strip=True)
            if "language" in label:
                detalhes["language"] = value
            elif "reading level" in label:
                detalhes["reading_level"] = value

        for a in soup.find_all("a", href=re.compile(r"/ebooks/subject/")):
            detalhes["subjects"].append(a.get_text(strip=True))

        resumo = soup.find("span", class_="toggle-content")
        if resumo:
            detalhes["summary"] = resumo.get_text(" ", strip=True)

        return detalhes


# ---------------- Demora demais, quis deixar mais rapido aqui. seu pc que lute ----------------
def executar_modo(modo):
    with open("categorias_filtradas.json", "r", encoding="utf-8") as f:
        categorias = json.load(f)

    db = BancoDeDados()
    resultados = []

    def processar_categoria(cat, paginas, limite=None):
        scraper = GutenbergScraper(cat["url"])
        return scraper.coletar_livros_categoria(paginas=paginas, limite_livros=limite)

    if modo == "FULL":
        with ThreadPoolExecutor(max_workers=8) as executor:  # usa até 8 threads
            futures = [executor.submit(processar_categoria, cat, 999) for cat in categorias]
            for future in as_completed(futures):
                livros = future.result()
                for livro in livros:
                    db.salvar_livro(livro)

    elif modo == "SMALL":
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(processar_categoria, cat, 1) for cat in categorias]
            for future in as_completed(futures):
                livros = future.result()
                for livro in livros:
                    db.salvar_livro(livro)

    elif modo == "TEST":
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(processar_categoria, cat, 1, 1) for cat in categorias[:10]]
            for future in as_completed(futures):
                livros = future.result()
                resultados.extend(livros)

        with open("livros_test.json", "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)
        print("✅ Arquivo 'livros_test.json' criado com dados de 10 categorias.")

    print("✅ Execução concluída.")


# ---------- Main ----------
if __name__ == "__main__":
    modo = input("Escolha o modo (FULL / SMALL / TEST): ").strip().upper()
    executar_modo(modo)
