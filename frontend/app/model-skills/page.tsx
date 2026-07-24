import type { Metadata } from "next";
import { ModelSkillLab } from "./skill-lab";

export const metadata: Metadata = {
  title: "Model Skill Lab · AuraClaw",
  description: "Test the config → Skill → MCP → Agent delivery path.",
};

export default function ModelSkillsPage() {
  return <ModelSkillLab />;
}
