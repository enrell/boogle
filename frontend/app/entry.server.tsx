import { PassThrough } from "node:stream";
import type { AppLoadContext, EntryContext } from "react-router";
import { createReadableStreamFromReadable } from "@react-router/node";
import { ServerRouter } from "react-router";
import { isbot } from "isbot";
import { renderToPipeableStream } from "react-dom/server.node";
import { createInstance } from "i18next";
import { I18nextProvider, initReactI18next } from "react-i18next";
import { resources } from "./i18n";

export const streamTimeout = 5_000;

export default async function handleRequest(
  request: Request,
  responseStatusCode: number,
  responseHeaders: Headers,
  routerContext: EntryContext,
  loadContext: AppLoadContext
) {
  // Detect language from cookie
  const cookieHeader = request.headers.get("Cookie");
  const match = cookieHeader?.match(/i18next=([a-z-]+)/i);

  let lng = "en";
  if (match) {
    lng = match[1];
  } else {
    // Fallback to Accept-Language
    const acceptLang = request.headers.get("Accept-Language");
    if (acceptLang) {
      // Simple parse: take first preferred language
      // Example: "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7" -> "pt-BR"
      const preferred = acceptLang.split(',')[0].split(';')[0].trim();

      // Check if exact match is supported
      if (resources[preferred as keyof typeof resources]) {
        lng = preferred;
      } else {
        // Try language code only (e.g., en-US -> en)
        const code = preferred.split('-')[0];
        if (resources[code as keyof typeof resources]) {
          lng = code;
        }
      }
    }
  }

  const i18n = createInstance();
  await i18n
    .use(initReactI18next)
    .init({
      lng,
      resources,
      fallbackLng: "en",
      supportedLngs: ["en", "pt", "pt-BR", "fr", "ja", "es"],
      interpolation: {
        escapeValue: false,
      },
      // Disable detection on server, we already manually set lng
      detection: {
        caches: []
      }
    });

  return new Promise((resolve, reject) => {
    let shellRendered = false;
    const userAgent = request.headers.get("user-agent");

    // Ensure requests from bots and SPA client side navigation are not processed
    // as streams, but wait for the full content to be ready.
    const readyOption =
      (userAgent && isbot(userAgent)) || routerContext.isSpaMode
        ? "onAllReady"
        : "onShellReady";

    const { pipe, abort } = renderToPipeableStream(
      <I18nextProvider i18n={i18n}>
        <ServerRouter context={routerContext} url={request.url} />
      </I18nextProvider>,
      {
        [readyOption]() {
          shellRendered = true;
          const body = new PassThrough();
          const stream = createReadableStreamFromReadable(body);

          responseHeaders.set("Content-Type", "text/html");

          resolve(
            new Response(stream, {
              headers: responseHeaders,
              status: responseStatusCode,
            })
          );

          pipe(body);
        },
        onShellError(error: unknown) {
          reject(error);
        },
        onError(error: unknown) {
          responseStatusCode = 500;
          // Log streaming rendering errors from inside the shell.  Don't log
          // errors encountered during initial shell rendering since they'll
          // reject and get logged in handleDocumentRequest.
          if (shellRendered) {
            console.error(error);
          }
        },
      }
    );

    setTimeout(abort, streamTimeout + 1000);
  });
}
