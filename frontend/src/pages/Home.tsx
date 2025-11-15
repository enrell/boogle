import { useState } from "react";
import { api } from "../api";
import { useNavigate } from 'react-router-dom';
import img from "../new.jpg";

function Search() {
    const [query, setQuery] = useState("");
    const [results, setResults] = useState<any[]>([]);
    const [loading, setLoading] = useState(false);
    const [mode, setMode] = useState<"all" | "title">("all");
    const navigate = useNavigate();

    const searchBooks = async () => {
        if (!query.trim())
            return;

        setLoading(true);

        try {
            const response = await api.get(`/search?query=${query}`);
            let data = response.data;

            if (mode === "title") {
                data = data.filter((book: any) =>
                    book.title.toLowerCase().includes(query.toLocaleLowerCase())
                );
            }
            setResults(data);

        } catch (error) {
            alert("Erro ao buscar livros.");
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen p-10" style={{ backgroundImage: `url(${img})`, backgroundSize: 'cover', 
                backgroundPosition: 'center',  
                backgroundRepeat: 'no-repeat',  
            }}>
            <div className="max-w-xl mx-auto bg-gray-100 opacity-90 p-6 shadow rounded-xl">
                <div className="">
                <h1 className="text-2xl font-bold mb-4 text-center text-red-800 outline-none">Buscador de Livros</h1>
                </div>

                 <div className="mb-4 mt-8">
                        <select
                            value={mode}
                            onChange={(e) => setMode(e.target.value as "all" | "title")}
                            className="p-2 border rounded outline-none cursor-pointer"
                        >
                            <option value="all"> Título, Autor, Descrição & Conteúdo</option>
                            <option value="title">Buscar apenas pelo título</option>
                        </select>
                    </div> 

                <div className="flex gap-2 mb-4">
                    <input
                        type="text"
                        placeholder="Digite o nome do livro"
                        className="w-full p-2 border rounded outline-none cursor-pointer"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                    />

                    <button
                        onClick={searchBooks}
                        className="outline-none bg-red-800 text-white px-4 py-2 rounded hover:bg-red-600"
                    >
                        Buscar
                    </button>
                </div>

                {loading && <p className="font-medium text-red-800">Carregando...</p>}

                {results.length === 0 && !loading && (
                    <p className="text-center text-gray-500">Nenhum resultado.</p>
                )}
            </div>
            {results.length > 0 && (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-6 p-4 mt-2 opacity-90">
                    
                        {results.map((book) => (
                            <div
                                key={book.book_id}
                                className="p-4 bg-gray-100 shadow rounded min-h-[100px] text-zinc-900 hover:bg-gray-200 cursor-pointer"
                                onClick={() => navigate (`/book/${book.book_id}`)}
                            >
                                <p className="text-lg font-bold">{book.title}</p>
                            </div>
                        ))}
                    
                </div>
                )}
        </div>
    );
}
export default Search;
