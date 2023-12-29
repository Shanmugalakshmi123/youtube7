import streamlit as st
from state import provide_state


@provide_state
def main(state):
    state.inputs = state.inputs or set()

    input_string = st.text_input("Give inputs")
    state.inputs.add(input_string)

    st.selectbox("Select Dynamic", options=list(state.inputs))

main()