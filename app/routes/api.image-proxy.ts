import type { LoaderFunctionArgs } from "react-router";

export async function loader({ request }: LoaderFunctionArgs) {
    const url = new URL(request.url);
    const imageUrl = url.searchParams.get("url");

    if (!imageUrl) {
        return new Response("Missing url param", { status: 400 });
    }

    try {
        const response = await fetch(imageUrl, {
            headers: {
                "User-Agent": "BoogleBot/1.0",
            },
        });

        if (!response.ok) {
            return new Response("Failed to fetch image", { status: response.status });
        }

        const contentType = response.headers.get("Content-Type") || "application/octet-stream";

        return new Response(response.body, {
            headers: {
                "Content-Type": contentType,
                "Cache-Control": "public, max-age=31536000, immutable",
                "Access-Control-Allow-Origin": "*",
            },
        });
    } catch (error) {
        console.error("Proxy error:", error);
        return new Response("Internal Server Error", { status: 500 });
    }
}
