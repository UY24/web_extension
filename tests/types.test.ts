import { describe, it, expect } from "vitest";
import type { Suggestion, ApplyEvent } from "../src/types";

describe("new extension types", () => {
  it("Suggestion has expected fields", () => {
    const s: Suggestion = {
      rowIndex: "1",
      suggestedCategory: "Operations:Software",
      categoryId: "cat1",
      confidence: 0.9,
      source: "llm",
      accepted: true,
    };
    expect(s.accepted).toBe(true);
  });

  it("ApplyEvent has expected fields", () => {
    const e: ApplyEvent = {
      rowIndex: "1",
      status: "applied",
      suggestedCategory: "Operations:Software",
      categoryId: null,
      confidence: 0.9,
      source: "llm",
      error: null,
      qboTransactionId: null,
    };
    expect(e.status).toBe("applied");
  });
});
