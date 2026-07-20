import type { Metadata } from "next";
import { AuraClawConsole } from "./workspace";

export const metadata: Metadata = {
  title: "AuraClaw Protocol Test Console",
  description: "Test streaming conversations, Query / Result flows, and AuraClaw operational signals.",
};

export default function Home() {
  return <AuraClawConsole />;
}
