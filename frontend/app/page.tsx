import type { Metadata } from "next";
import { AuraClawConsole } from "./workspace";

export const metadata: Metadata = {
  title: "AuraClaw Operations Console",
  description: "A browser-based task testing and observability console for AuraClaw.",
};

export default function Home() {
  return <AuraClawConsole />;
}
