//! Attribute argument parsing, shared by `#[main]` and `#[test]`.
//!
//! Three keys, deliberately: `placement`, `name` and `crate`. Anything the
//! builder does better stays on the builder.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Ident, LitStr, Path, Token,
};

pub(crate) struct Args {
    /// Fully qualified, e.g. `::glommio::Placement::Fixed(0)`.
    pub(crate) placement: TokenStream,
    pub(crate) name: Option<LitStr>,
    pub(crate) krate: Path,
}

enum Arg {
    Placement(TokenStream),
    Name(LitStr),
    Crate(Path),
}

impl Parse for Arg {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        // `crate` is a keyword, so it does not parse as an Ident.
        if input.peek(Token![crate]) {
            input.parse::<Token![crate]>()?;
            input.parse::<Token![=]>()?;
            return Ok(Arg::Crate(input.parse()?));
        }

        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;

        match key.to_string().as_str() {
            "placement" => {
                let variant: syn::Expr = input.parse()?;
                match &variant {
                    syn::Expr::Path(_) | syn::Expr::Call(_) => {}
                    _ => {
                        return Err(syn::Error::new_spanned(
                            &variant,
                            "placement takes a Placement variant such as `Unbound`, `Fixed(0)` \
                             or `Fenced(cpus)`, not an arbitrary expression. Build the executor \
                             with LocalExecutorBuilder if you need a computed placement",
                        ))
                    }
                }
                Ok(Arg::Placement(quote!(#variant)))
            }
            "name" => Ok(Arg::Name(input.parse()?)),
            other => Err(syn::Error::new_spanned(
                &key,
                format!("unknown argument `{other}`; expected `placement` or `crate`"),
            )),
        }
    }
}

/// Parses the attribute arguments, defaulting the placement to
/// `default_placement` when the caller did not name one.
pub(crate) fn parse(args: proc_macro::TokenStream, default_placement: &str) -> syn::Result<Args> {
    let parsed = syn::parse::Parser::parse(Punctuated::<Arg, Token![,]>::parse_terminated, args)?;

    let mut placement = None;
    let mut name = None;
    let mut krate = None;

    for arg in parsed {
        match arg {
            Arg::Placement(value) => placement = Some(value),
            Arg::Name(value) => name = Some(value),
            Arg::Crate(value) => krate = Some(value),
        }
    }

    let krate = krate.unwrap_or_else(|| syn::parse_quote!(::glommio));
    let placement = match placement {
        Some(variant) => quote!(#krate::Placement::#variant),
        None => {
            let default: Ident = Ident::new(default_placement, proc_macro2::Span::call_site());
            quote!(#krate::Placement::#default)
        }
    };

    Ok(Args {
        placement,
        name,
        krate,
    })
}
