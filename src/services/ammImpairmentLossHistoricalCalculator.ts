export interface AmmHistoricalPriceSnapshot {
  timestamp: Date;
  priceA: number;
  priceB: number;
}

export interface AmmHistoricalPriceSource {
  getPriceHistory(
    poolId: string,
    from: Date,
    to: Date,
  ): Promise<AmmHistoricalPriceSnapshot[]>;
}

export interface AmmInitialPosition {
  poolId: string;
  reserveA: number;
  reserveB: number;
}

export interface AmmImpairmentPoint extends AmmHistoricalPriceSnapshot {
  poolId: string;
  holdValue: number;
  ammValue: number;
  impairmentLoss: number;
  impermanentLossPercent: number;
}

function assertPositiveFinite(value: number, name: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a finite positive number`);
  }
}

/** Calculates historical constant-product LP impairment against holding. */
export class AmmImpairmentLossHistoricalCalculator {
  constructor(private readonly source: AmmHistoricalPriceSource) {}

  async calculate(
    position: AmmInitialPosition,
    from: Date,
    to: Date,
  ): Promise<AmmImpairmentPoint[]> {
    if (!position.poolId.trim()) throw new Error("poolId is required");
    assertPositiveFinite(position.reserveA, "reserveA");
    assertPositiveFinite(position.reserveB, "reserveB");
    if (
      !Number.isFinite(from.getTime()) ||
      !Number.isFinite(to.getTime()) ||
      from > to
    ) {
      throw new Error("Historical range must contain valid ordered dates");
    }

    const history = await this.source.getPriceHistory(
      position.poolId,
      from,
      to,
    );
    const invariant = position.reserveA * position.reserveB;

    return [...history]
      .sort((left, right) => left.timestamp.getTime() - right.timestamp.getTime())
      .map((snapshot) => {
        if (!Number.isFinite(snapshot.timestamp.getTime())) {
          throw new Error("Price history contains an invalid timestamp");
        }
        assertPositiveFinite(snapshot.priceA, "priceA");
        assertPositiveFinite(snapshot.priceB, "priceB");

        const holdValue =
          position.reserveA * snapshot.priceA +
          position.reserveB * snapshot.priceB;
        const ammValue =
          2 *
          Math.sqrt(
            invariant * snapshot.priceA * snapshot.priceB,
          );

        return {
          poolId: position.poolId,
          timestamp: snapshot.timestamp,
          priceA: snapshot.priceA,
          priceB: snapshot.priceB,
          holdValue,
          ammValue,
          impairmentLoss: Math.max(0, holdValue - ammValue),
          impermanentLossPercent: ((ammValue / holdValue) - 1) * 100,
        };
      });
  }
}