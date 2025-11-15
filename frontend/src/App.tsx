import React from 'react';
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Book from "./pages/Home";
import BookDetails from "./pages/BookDetails";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Book />} />
        <Route path="/book/:id" element={<BookDetails />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
