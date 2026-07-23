# Snowflake serving veneer + the cost guardrails.
#
# WHY THESE ARE IaC AND NOT CONSOLE SETTINGS. A warehouse that fails to auto-suspend is the
# single most expensive mistake available in this stack, and a console toggle has no review, no
# diff and no record of who changed it. Expressed here, a change to the idle window or the credit
# ceiling shows up as a plan diff on a pull request before it can cost anything.

resource "snowflake_warehouse" "serving" {
  name           = var.serving_warehouse_name
  warehouse_size = "XSMALL"
  comment        = "Serves Gold external tables to BI. Sized for a portfolio workload."

  # The guardrail. Idle compute is the default failure mode of a demo warehouse nobody is
  # watching; 60s means a forgotten dashboard costs seconds, not hours.
  auto_suspend = var.serving_warehouse_auto_suspend_seconds
  auto_resume  = true

  initially_suspended = true

  # Keep a runaway query from holding compute open indefinitely.
  statement_timeout_in_seconds = 3600

  # Provider v1 attaches the monitor from the warehouse side.
  resource_monitor = snowflake_resource_monitor.serving.name
}

# The ceiling behind the guardrail. auto_suspend limits idle burn; the monitor limits total burn,
# including the case where something legitimately runs hot. NOTIFY first, then suspend — so the
# warning arrives before the serving layer disappears mid-demo.
resource "snowflake_resource_monitor" "serving" {
  name         = "BANKING_SERVING_MONITOR"
  credit_quota = var.monthly_credit_quota
  # frequency defaults to MONTHLY; setting it explicitly would also require start_timestamp,
  # which would pin this config to a fixed date for no benefit.

  notify_triggers           = [75, 90]
  suspend_trigger           = 95
  suspend_immediate_trigger = 100

}
