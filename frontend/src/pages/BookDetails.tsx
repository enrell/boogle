import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import img from "../new.jpg";
import svg from "../landscape-placeholder.svg";

function BookDetails() {
    const { id } = useParams();
    const [book, setBook] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const navigate = useNavigate();

    const backPage = () => {
        navigate('/');
    }

    useEffect(() => {
        const fetchBook = async () => {
            try {
                const response = await api.get(`/metadata/${id}`);
                setBook(response.data);
            } catch (error) {
                console.error("Erro ao carregar livro", error);
            } finally {
                setLoading(false);
            }
        };

        fetchBook();
    }, [id]);

    if (loading) return <p className="font-medium text-gray-400 text-center">Carregando...</p>;

    if (!book) return <p className="font-medium text-gray-400 text-center">Livro não encontrado.</p>;

    return (
        <>
            <div className="min-h-screen" style={{
                backgroundImage: `url(${img})`, backgroundSize: 'cover',
                backgroundPosition: 'center',
                backgroundRepeat: 'no-repeat',
            }}>
                <div className="pt-5 mx-5">
                    <button className="outline-none bg-red-800 text-white px-4 py-2 rounded hover:bg-red-600" onClick={(backPage)}>
                        Voltar
                    </button>
                </div>
                <div className="p-5 gap-10 max-w-4xl mx-auto flex flex-col items-center flex-wrap md:items-start md:flex md:flex-row md:justify-center text-white">
                    <div className="flex flex-col items-center gap-5">
                        <img src={book.image_url ? book.image_url : svg} alt="capa do livro" className="object-cover w-64 h-64 md:w-80 md:h-80 rounded-md" />
                        <h1 className="md:text-3xl text-2xl font-bold mb-4 max-w-xs md:max-w-sm break-words text-center ">{book.title}</h1>
                    </div>

                    <div className="flex flex-col items-center gap-5 w-72 md:w-auto">
                        <div className="w-56 flex justify-center bg-red-800 border-none rounded-md py-1 px-1">
                        <h2 className="flex flex-col md:text-start text-center text-xl font-bold text-gray-100 opacity-90"> Informações do livro </h2>
                        </div>
                        <div className="flex flex-col gap-5">
                        <p><strong className="p-1 rounded-md">ID:</strong> {book.book_id}</p>
                        <p><strong className="p-1 rounded-r-md">Autor:</strong> {book.author || "Não informado"}</p>
                        <p><strong className="p-1 rounded-r-md">Idioma:</strong> {book.language}</p>
                        <p><strong className="p-1 rounded-r-md">Categoria:</strong> {book.category}</p>
                        <p><strong className="p-1 rounded-r-md">Data de Publicação:</strong> {book.release_date}</p>
                        <p className="break-words max-w-xs"><strong className=" p-1 rounded-r-md">Creditos</strong> {book.credits}</p>
                        <p><strong className=" p-1 rounded-r-md">Downloads:</strong> {book.downloads}</p>
                        </div>
                    </div>
                </div>
                <div className="flex flex-col items-center">
                    <h2 className="flex flex-col text-xl font-bold mt-8 text-gray-100 py-1 px-2 bg-red-800 rounded-md opacity-90">Arquivos disponíveis:</h2>
                    <ul className="mt-3 flex flex-col gap-5">
                        {book.files.map((f: any) => (
                            <li key={f.url} className="mt-2">
                                <a className="text-blue-600 underline" href={f.url} target="_blank">
                                    {f.format}
                                </a>
                            </li>
                        ))}
                    </ul>
                </div>
            </div>
        </>
    );
}

export default BookDetails;
