import { describe, expect, it, jest } from "@jest/globals";
import {
  Account,
  Address,
  Contract,
  Keypair,
  Networks,
  Operation,
  TransactionBuilder,
  nativeToScVal,
  rpc as SorobanRpc,
  xdr,
} from "@stellar/stellar-sdk";
import { AmmImpairmentLossHistoricalCalculator } from "../src/services/ammImpairmentLossHistoricalCalculator";
import { OracleStalePriceAlertService } from "../src/services/oracleStalePriceAlertService";
import { S3LogArchiveLifecycleConfigurator } from "../src/services/s3LogArchiveLifecycleConfigurator";
import { SorobanContractPreflightInspector } from "../src/services/sorobanContractPreflightInspector";

const contractId = "CA3D5KRYM6CB7OWQ6TWYRR3Z4T7GNZLKERYNZGGA5SOAOPIFY6YQGAXE";
const source = Keypair.fromRawEd25519Seed(Buffer.alloc(32, 7)).publicKey();

function contractDataKey(key: string): xdr.LedgerKey {
  return xdr.LedgerKey.contractData(
    new xdr.LedgerKeyContractData({
      contract: new Address(contractId).toScAddress(),
      key: nativeToScVal(key),
      durability: xdr.ContractDataDurability.persistent(),
    }),
  );
}

function sorobanTransaction(
  readOnly: xdr.LedgerKey[],
  readWrite: xdr.LedgerKey[],
) {
  const sorobanData = new xdr.SorobanTransactionData({
    ext: new xdr.SorobanTransactionDataExt(0),
    resources: new xdr.SorobanResources({
      footprint: new xdr.LedgerFootprint({ readOnly, readWrite }),
      instructions: 100,
      diskReadBytes: 100,
      writeBytes: 100,
    }),
    resourceFee: new xdr.Int64(100),
  });
  return new TransactionBuilder(new Account(source, "1"), {
    fee: "100000",
    networkPassphrase: Networks.TESTNET,
  })
    .addOperation(
      new Contract(contractId).call("read", nativeToScVal("test")),
    )
    .setSorobanData(sorobanData)
    .setTimeout(30)
    .build();
}

function classicTransaction() {
  return new TransactionBuilder(new Account(source, "1"), {
    fee: "100",
    networkPassphrase: Networks.TESTNET,
  })
    .addOperation(Operation.manageData({ name: "PRICE", value: "1" }))
    .setTimeout(30)
    .build();
}

describe("requested service issues", () => {
  it("calculates historical AMM impairment against holding", async () => {
    const calculator = new AmmImpairmentLossHistoricalCalculator({
      getPriceHistory: async () => [
        { timestamp: new Date("2026-01-02"), priceA: 4, priceB: 1 },
        { timestamp: new Date("2026-01-01"), priceA: 1, priceB: 1 },
      ],
    });

    const result = await calculator.calculate(
      { poolId: "pool-1", reserveA: 100, reserveB: 100 },
      new Date("2026-01-01"),
      new Date("2026-01-03"),
    );

    expect(result[0]?.ammValue).toBeCloseTo(200);
    expect(result[1]?.holdValue).toBeCloseTo(500);
    expect(result[1]?.ammValue).toBeCloseTo(400);
    expect(result[1]?.impairmentLoss).toBeCloseTo(100);
    expect(result[1]?.impermanentLossPercent).toBeCloseTo(-20);
  });

  it("builds archive lifecycle rules and protects Object Lock retention", async () => {
    const configurator = new S3LogArchiveLifecycleConfigurator({
      send: jest.fn(),
    });
    const rule = configurator.buildRule({
      bucketName: "logs-prod",
      prefix: "application/",
      expirationDays: 365,
      objectLockRetentionDays: 180,
    });

    expect(rule.Filter).toEqual({ Prefix: "application/" });
    expect(rule.Transitions).toEqual([
      { Days: 30, StorageClass: "STANDARD_IA" },
      { Days: 90, StorageClass: "GLACIER" },
    ]);
    expect(() =>
      configurator.buildRule({
        bucketName: "logs-prod",
        expirationDays: 30,
        objectLockRetentionDays: 90,
      }),
    ).toThrow(/Object Lock retention/);
  });

  it("preserves unrelated bucket lifecycle rules when configuring", async () => {
    const unrelatedRule = { ID: "keep-me", Status: "Enabled" as const };
    const send = jest
      .fn()
      .mockResolvedValueOnce({ Rules: [unrelatedRule] })
      .mockResolvedValueOnce({});
    const configurator = new S3LogArchiveLifecycleConfigurator({ send });

    await configurator.configure({ bucketName: "logs-prod" });

    const putCommand = send.mock.calls[1]?.[0] as {
      input: { LifecycleConfiguration: { Rules: Array<{ ID?: string }> } };
    };
    expect(putCommand.input.LifecycleConfiguration.Rules.map((rule) => rule.ID)).toEqual([
      "keep-me",
      "stellarflow-log-archive-lifecycle",
    ]);
  });

  it("triggers once per stale-price episode and recovers on a fresh price", async () => {
    const sendAlert = jest.fn().mockResolvedValue(true);
    const onEmergencyFallback = jest.fn().mockResolvedValue(undefined);
    const monitor = new OracleStalePriceAlertService(
      { sendAlert } as any,
      { staleAfterMs: 1_000, onEmergencyFallback },
    );
    const observedAt = new Date("2026-01-01T00:00:00.000Z");

    monitor.observePrice({ asset: "xlm", price: 0.12, observedAt });
    expect(
      await monitor.checkStale(new Date(observedAt.getTime() + 1_001)),
    ).toHaveLength(1);
    expect(
      await monitor.checkStale(new Date(observedAt.getTime() + 2_000)),
    ).toHaveLength(0);
    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(onEmergencyFallback).toHaveBeenCalledTimes(1);

    monitor.observePrice({
      asset: "XLM",
      price: 0.13,
      observedAt: new Date(observedAt.getTime() + 2_001),
    });
    expect(
      await monitor.checkStale(new Date(observedAt.getTime() + 3_002)),
    ).toHaveLength(1);
    expect(onEmergencyFallback).toHaveBeenCalledTimes(2);
    monitor.stop();
  });

  it("classifies exact Soroban read-only and read-write footprint keys", () => {
    const inspector = new SorobanContractPreflightInspector(
      {} as SorobanRpc.Server,
    );
    const readOnly = contractDataKey("balance");
    const readWrite = contractDataKey("supply");

    const footprint = inspector.inspectFootprint(
      sorobanTransaction([readOnly], [readWrite]),
    );

    expect(footprint.readOnly).toHaveLength(1);
    expect(footprint.readOnly[0]?.keyType).toBe("contractData");
    expect(footprint.readOnly[0]?.keyXdr).toBe(readOnly.toXDR("base64"));
    expect(footprint.readWrite).toHaveLength(1);
    expect(footprint.readWrite[0]?.keyXdr).toBe(readWrite.toXDR("base64"));
  });

  it("does not reject classic transactions for lacking Soroban footprints", () => {
    const inspector = new SorobanContractPreflightInspector(
      {} as SorobanRpc.Server,
    );
    expect(inspector.inspectFootprint(classicTransaction())).toBeNull();
  });
});