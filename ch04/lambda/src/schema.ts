import { pgTable, integer, uuid, timestamp } from "drizzle-orm/pg-core";

// Accounts table - uses integer PK (low write rate, reference data)
export const accounts = pgTable("accounts", {
  id: integer("id").primaryKey(),
  balance: integer("balance").notNull(),
});

// Transactions table - uses UUID PK (high write rate, avoids hotspots)
export const transactions = pgTable("transactions", {
  id: uuid("id").defaultRandom().primaryKey(),
  payerId: integer("payer_id").notNull(),
  payeeId: integer("payee_id").notNull(),
  amount: integer("amount").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});
