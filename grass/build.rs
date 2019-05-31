use vergen::{ConstantsFlags, generate_cargo_keys};

fn main() {
	let mut flags = ConstantsFlags::empty();
    flags.toggle(ConstantsFlags::SHA_SHORT);
	flags.toggle(ConstantsFlags::COMMIT_DATE);
	flags.toggle(ConstantsFlags::TARGET_TRIPLE);

    // Generate the 'cargo:' key output
    generate_cargo_keys(ConstantsFlags::all()).expect("Unable to generate the cargo keys");
}
