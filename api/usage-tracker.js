/**
 * Usage Tracker & Quota Protection System
 * Monitors Vercel Hobby account limits and auto-stops when approaching limits
 * 
 * Vercel Hobby Limits:
 * - 100 GB Bandwidth per month
 * - 100 GB-Hours Function execution per month
 * - 1000 Serverless Function invocations per day
 */

export class UsageTracker {
  constructor() {
    // Monthly limits (Vercel Hobby)
    this.MONTHLY_BANDWIDTH_LIMIT = 100 * 1024 * 1024 * 1024; // 100 GB
    this.MONTHLY_EXECUTION_LIMIT = 100 * 3600 * 1000; // 100 GB-Hours in ms
    this.DAILY_INVOCATION_LIMIT = 1000;
    
    // Safety thresholds (stop before hitting limits)
    this.BANDWIDTH_THRESHOLD = 0.85; // Stop at 85%
    this.EXECUTION_THRESHOLD = 0.85; // Stop at 85%
    this.INVOCATION_THRESHOLD = 0.90; // Stop at 90%
    
    // Current usage (reset monthly/daily)
    this.currentMonth = new Date().getMonth();
    this.currentDay = new Date().getDate();
    this.bandwidthUsed = 0;
    this.executionTimeUsed = 0;
    this.invocationsToday = 0;
    this.totalInvocations = 0;
    
    // Status
    this.isActive = true;
    this.pauseReason = null;
    this.pausedAt = null;
    this.resumeAt = null;
    
    // Load persisted data if available
    this.loadState();
    
    // Auto-check and reset
    this.checkAndReset();
  }
  
  loadState() {
    // In production, this would load from a persistent store (KV, database, etc.)
    // For now, we use in-memory storage that resets on cold starts
    if (global.usageTrackerState) {
      Object.assign(this, global.usageTrackerState);
    }
  }
  
  saveState() {
    // Persist state
    global.usageTrackerState = {
      currentMonth: this.currentMonth,
      currentDay: this.currentDay,
      bandwidthUsed: this.bandwidthUsed,
      executionTimeUsed: this.executionTimeUsed,
      invocationsToday: this.invocationsToday,
      totalInvocations: this.totalInvocations,
      isActive: this.isActive,
      pauseReason: this.pauseReason,
      pausedAt: this.pausedAt,
      resumeAt: this.resumeAt,
    };
  }
  
  checkAndReset() {
    const now = new Date();
    const currentMonth = now.getMonth();
    const currentDay = now.getDate();
    
    // Reset monthly counters
    if (currentMonth !== this.currentMonth) {
      this.currentMonth = currentMonth;
      this.bandwidthUsed = 0;
      this.executionTimeUsed = 0;
      this.totalInvocations = 0;
      this.isActive = true;
      this.pauseReason = null;
      this.pausedAt = null;
      this.resumeAt = null;
      console.log('[UsageTracker] Monthly reset completed');
    }
    
    // Reset daily counters
    if (currentDay !== this.currentDay) {
      this.currentDay = currentDay;
      this.invocationsToday = 0;
      
      // Check if we should resume after daily limit
      if (this.pauseReason === 'daily_invocation_limit') {
        this.isActive = true;
        this.pauseReason = null;
        this.pausedAt = null;
        this.resumeAt = null;
        console.log('[UsageTracker] Daily reset - service resumed');
      }
    }
    
    this.saveState();
  }
  
  trackRequest(bytesTransferred, executionTimeMs) {
    this.checkAndReset();
    
    // Update counters
    this.bandwidthUsed += bytesTransferred;
    this.executionTimeUsed += executionTimeMs;
    this.invocationsToday += 1;
    this.totalInvocations += 1;
    
    // Check limits
    this.checkLimits();
    
    this.saveState();
  }
  
  checkLimits() {
    const bandwidthPercent = this.bandwidthUsed / this.MONTHLY_BANDWIDTH_LIMIT;
    const executionPercent = this.executionTimeUsed / this.MONTHLY_EXECUTION_LIMIT;
    const invocationPercent = this.invocationsToday / this.DAILY_INVOCATION_LIMIT;
    
    // Check bandwidth limit
    if (bandwidthPercent >= this.BANDWIDTH_THRESHOLD) {
      this.pause('bandwidth_limit', this.getNextMonthStart());
      return;
    }
    
    // Check execution time limit
    if (executionPercent >= this.EXECUTION_THRESHOLD) {
      this.pause('execution_limit', this.getNextMonthStart());
      return;
    }
    
    // Check daily invocation limit
    if (invocationPercent >= this.INVOCATION_THRESHOLD) {
      this.pause('daily_invocation_limit', this.getNextDayStart());
      return;
    }
  }
  
  pause(reason, resumeAt) {
    this.isActive = false;
    this.pauseReason = reason;
    this.pausedAt = new Date().toISOString();
    this.resumeAt = resumeAt;
    
    console.log(`[UsageTracker] Service paused: ${reason}`);
    console.log(`[UsageTracker] Will resume at: ${resumeAt}`);
    
    this.saveState();
  }
  
  getNextMonthStart() {
    const now = new Date();
    const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    return nextMonth.toISOString();
  }
  
  getNextDayStart() {
    const now = new Date();
    const nextDay = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
    return nextDay.toISOString();
  }
  
  canAcceptRequest() {
    this.checkAndReset();
    return this.isActive;
  }
  
  getStatus() {
    this.checkAndReset();
    
    const bandwidthPercent = (this.bandwidthUsed / this.MONTHLY_BANDWIDTH_LIMIT * 100).toFixed(2);
    const executionPercent = (this.executionTimeUsed / this.MONTHLY_EXECUTION_LIMIT * 100).toFixed(2);
    const invocationPercent = (this.invocationsToday / this.DAILY_INVOCATION_LIMIT * 100).toFixed(2);
    
    return {
      active: this.isActive,
      pauseReason: this.pauseReason,
      pausedAt: this.pausedAt,
      resumeAt: this.resumeAt,
      usage: {
        bandwidth: {
          used: this.formatBytes(this.bandwidthUsed),
          limit: this.formatBytes(this.MONTHLY_BANDWIDTH_LIMIT),
          percent: `${bandwidthPercent}%`,
          remaining: this.formatBytes(this.MONTHLY_BANDWIDTH_LIMIT - this.bandwidthUsed),
        },
        execution: {
          used: `${(this.executionTimeUsed / 3600000).toFixed(2)} GB-Hours`,
          limit: '100 GB-Hours',
          percent: `${executionPercent}%`,
        },
        invocations: {
          today: this.invocationsToday,
          total: this.totalInvocations,
          dailyLimit: this.DAILY_INVOCATION_LIMIT,
          percent: `${invocationPercent}%`,
        },
      },
      thresholds: {
        bandwidth: `${(this.BANDWIDTH_THRESHOLD * 100)}%`,
        execution: `${(this.EXECUTION_THRESHOLD * 100)}%`,
        invocation: `${(this.INVOCATION_THRESHOLD * 100)}%`,
      },
    };
  }
  
  formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
  }
  
  getPauseMessage() {
    if (!this.pauseReason) return null;
    
    const messages = {
      bandwidth_limit: 'Service temporarily paused: Monthly bandwidth limit reached (85% of 100 GB). Will resume next month.',
      execution_limit: 'Service temporarily paused: Monthly execution time limit reached (85% of 100 GB-Hours). Will resume next month.',
      daily_invocation_limit: 'Service temporarily paused: Daily invocation limit reached (90% of 1000). Will resume tomorrow.',
    };
    
    return {
      message: messages[this.pauseReason] || 'Service temporarily paused',
      resumeAt: this.resumeAt,
      reason: this.pauseReason,
    };
  }
}

// Singleton instance
let trackerInstance = null;

export function getUsageTracker() {
  if (!trackerInstance) {
    trackerInstance = new UsageTracker();
  }
  return trackerInstance;
}

// Made with Bob
