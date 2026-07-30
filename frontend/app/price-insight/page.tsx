import type { Metadata } from "next";
import { PriceInsightLab } from "./price-insight-lab";

export const metadata: Metadata = {
  title: "价格洞察智能体 · AuraClaw",
  description: "通过真实 MySQL DWD 调试价格洞察 Skill、Tool 与 Agent Loop。",
};

export default function PriceInsightPage() {
  return <PriceInsightLab />;
}
