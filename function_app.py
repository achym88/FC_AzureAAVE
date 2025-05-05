import azure.functions as func
from aave.aave_logic import aave_liquidity_storage_impl
from eth.eth_logic import eth_liquidity_storage_impl

app = func.FunctionApp()

@app.schedule(schedule="0 */3 * * * *", arg_name="timer")
async def aave_liquidity_storage(timer: func.TimerRequest) -> None:
    await aave_liquidity_storage_impl(timer)

@app.schedule(schedule="0 */3 * * * *", arg_name="timer")
async def eth_liquidity_storage(timer: func.TimerRequest) -> None:
    await eth_liquidity_storage_impl(timer)