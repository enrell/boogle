import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/home.tsx"),
  route("search", "routes/search.tsx"),
  route("book/:id", "routes/book.$id.tsx"),
  route("api/book/:id", "routes/api.book.$id.ts"),
  route("api/image-proxy", "routes/api.image-proxy.ts"),
] satisfies RouteConfig;
