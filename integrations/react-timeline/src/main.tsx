import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles.css";

const rootElement = document.getElementById("root");
if (rootElement === null) {
  throw new Error('index.html is missing the #root element this app mounts into.');
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
