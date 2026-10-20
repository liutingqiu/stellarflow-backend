import { broadcastToSessions } from "../lib/socket";
import { getRedisClient } from "../lib/redis";
import {
  AlertSeverity,
  AlertType,
  NotificationService,
} from "./notificationService";

export interface OraclePriceObservation {
  asset: string;
  price: number;
  observedAt: Date;
  source?: string;
}

export interface OracleStalePriceAlert {
  asset: string;
  lastPrice: number;
  lastUpdatedAt: string;
  staleDurationMs: number;
  triggeredAt: string;
  source?: string;
}

export interface OracleStalePriceAlertOptions {
  staleAfterMs?: number;
  checkIntervalMs?: number;
  onEmergencyFallback?: (alert: OracleStalePriceAlert) => Promise<void>;
}

interface TrackedPrice extends OraclePriceObservation {
  fallbackTriggered: boolean;
}

/** Alerts when accepted oracle prices go stale and signals emergency fallback consumers. */
export class OracleStalePriceAlertService {
  private readonly prices = new Map<string, TrackedPrice>();
  private readonly staleAfterMs: number;
  private readonly checkIntervalMs: number;
  private readonly onEmergencyFallback: (
    alert: OracleStalePriceAlert,
  ) => Promise<void>;
  private timer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly notifications = new NotificationService(),
    options: OracleStalePriceAlertOptions = {},
  ) {
    this.staleAfterMs =
      options.staleAfterMs ??
      Number(process.env.ORACLE_STALE_AFTER_MS ?? "120000");
    this.checkIntervalMs =
      options.checkIntervalMs ??
      Number(process.env.ORACLE_STALE_CHECK_INTERVAL_MS ?? "10000");
    if (
      !Number.isFinite(this.staleAfterMs) ||
      this.staleAfterMs <= 0 ||
      !Number.isFinite(this.checkIntervalMs) ||
      this.checkIntervalMs <= 0
    ) {
      throw new Error("Oracle stale timing values must be positive numbers");
    }
    this.onEmergencyFallback =
      options.onEmergencyFallback ?? ((alert) => this.dispatchFallback(alert));
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.checkStale().catch((error) => {
        console.error("[OracleStalePriceAlert] Stale-price check failed", error);
      });
    }, this.checkIntervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  observePrice(observation: OraclePriceObservation): void {
    const asset = observation.asset.trim().toUpperCase();
    if (!asset) throw new Error("asset is required");
    if (!Number.isFinite(observation.price) || observation.price <= 0) {
      throw new Error("price must be a finite positive number");
    }
    if (!Number.isFinite(observation.observedAt.getTime())) {
      throw new Error("observedAt must be a valid date");
    }

    const previous = this.prices.get(asset);
    if (
      previous &&
      observation.observedAt.getTime() < previous.observedAt.getTime()
    ) {
      return;
    }

    this.prices.set(asset, {
      ...observation,
      asset,
      fallbackTriggered: false,
    });
  }

  async checkStale(now = new Date()): Promise<OracleStalePriceAlert[]> {
    const alerts: OracleStalePriceAlert[] = [];
    for (const tracked of this.prices.values()) {
      const staleDurationMs = now.getTime() - tracked.observedAt.getTime();
      if (staleDurationMs < this.staleAfterMs || tracked.fallbackTriggered) {
        continue;
      }

      tracked.fallbackTriggered = true;
      const alert: OracleStalePriceAlert = {
        asset: tracked.asset,
        lastPrice: tracked.price,
        lastUpdatedAt: tracked.observedAt.toISOString(),
        staleDurationMs,
        triggeredAt: now.toISOString(),
        ...(tracked.source ? { source: tracked.source } : {}),
      };
      alerts.push(alert);
      try {
        await this.notifications.sendAlert({
          type: AlertType.ORACLE_STALE_PRICE,
          severity: AlertSeverity.CRITICAL,
          title: `Oracle price is stale for ${tracked.asset}`,
          message: `No accepted price update for ${tracked.asset} in ${Math.round(staleDurationMs / 1000)} seconds. Emergency fallback was triggered.`,
          details: alert,
          timestamp: now,
          service: "oracle-stale-price-monitor",
        });
      } catch (error) {
        console.error("[OracleStalePriceAlert] Alert delivery failed", error);
      }
      try {
        await this.onEmergencyFallback(alert);
      } catch (error) {
        console.error(
          `[OracleStalePriceAlert] Emergency fallback trigger failed for ${tracked.asset}`,
          error,
        );
      }
    }
    return alerts;
  }

  private async dispatchFallback(alert: OracleStalePriceAlert): Promise<void> {
    broadcastToSessions("oracle:stale_price", alert);
    broadcastToSessions("oracle:fallback_trigger", alert);

    try {
      const redis = getRedisClient();
      if (redis?.isOpen) {
        await redis.xAdd("events:oracle-fallback-trigger", "*", {
          payload: JSON.stringify(alert),
        });
      }
    } catch (error) {
      console.error("[OracleStalePriceAlert] Fallback event publish failed", error);
    }
  }
}

export const oracleStalePriceAlertService =
  new OracleStalePriceAlertService();