//! `select!`: race several futures, take the first to finish.
//!
//! Like the attribute macros, every path this emits goes through `krate` so a
//! caller can point it at glommio under another name. See the crate-level
//! note: a hardcoded `::glommio` here is a compile error in somebody else's
//! code, and it has already happened twice.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    Expr, Ident, Pat, Token,
};

/// One `pat = future => body` branch.
struct Branch {
    pattern: Pat,
    future: Expr,
    body: Expr,
}

/// Whether a branch body must be followed by a comma.
///
/// The same rule `match` arms use: a body that ends in a block stands on its
/// own, anything else needs separating from the branch that follows. tokio's
/// `select!` follows `match` here, so code written against it omits the comma
/// after block bodies -- and requiring one produced an error pointing at the
/// *next* branch's `=>`, which reads as a complaint about that branch's
/// pattern rather than a missing token on the line above.
fn needs_trailing_comma(body: &Expr) -> bool {
    !matches!(
        body,
        Expr::Block(_)
            | Expr::If(_)
            | Expr::Match(_)
            | Expr::ForLoop(_)
            | Expr::While(_)
            | Expr::Loop(_)
            | Expr::Unsafe(_)
            | Expr::TryBlock(_)
            | Expr::Const(_)
    )
}

impl Parse for Branch {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        // Refutable patterns are rejected: tokio disables a non-matching
        // branch and keeps polling, which is a surprising rule to reimplement
        // and nothing here needs it. `parse_single` accepts the irrefutable
        // forms and rejects `|` alternatives outright.
        let pattern = Pat::parse_single(input)?;
        input.parse::<Token![=]>()?;
        let future: Expr = input.parse()?;
        input.parse::<Token![=>]>()?;

        // A braced body is parsed as a block, not handed to the general
        // expression parser. `Expr::parse` would read `{ .. } (` as a call --
        // `{block}()` -- and swallow the following branch, which is what made
        // the missing-comma error point at the *next* branch's pattern.
        let body = if input.peek(syn::token::Brace) {
            Expr::Block(syn::ExprBlock {
                attrs: Vec::new(),
                label: None,
                block: input.parse()?,
            })
        } else {
            input.parse()?
        };

        Ok(Branch {
            pattern,
            future,
            body,
        })
    }
}

struct Select {
    biased: bool,
    /// The runtime crate's path. Anyone depending on glommio under another
    /// name -- or reaching it through a facade -- must be able to say so, the
    /// same way `#[glommio::main]` accepts `crate = …`.
    krate: syn::Path,
    branches: Vec<Branch>,
}

impl Parse for Select {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut biased = false;
        let mut krate: Option<syn::Path> = None;

        // Leading directives, in either order, each ended with `;`. `crate` is
        // a keyword, so it can never be mistaken for a branch pattern.
        loop {
            if input.peek(Token![crate]) {
                input.parse::<Token![crate]>()?;
                input.parse::<Token![=]>()?;
                krate = Some(input.parse()?);
                input.parse::<Token![;]>()?;
                continue;
            }

            if input.peek(Ident) && input.fork().parse::<Ident>()? == "biased" {
                input.parse::<Ident>()?;
                input.parse::<Token![;]>()?;
                biased = true;
                continue;
            }

            break;
        }

        let krate = krate.unwrap_or_else(|| syn::parse_quote!(::glommio));

        let mut branches = Vec::new();
        while !input.is_empty() {
            let branch: Branch = input.parse()?;
            let comma_required = needs_trailing_comma(&branch.body);
            branches.push(branch);

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
                continue;
            }

            // Trailing comma is always optional on the last branch.
            if input.is_empty() {
                break;
            }

            if comma_required {
                // Reported here, against the body that needs separating,
                // rather than letting the next branch fail to parse and blame
                // its own pattern.
                return Err(syn::Error::new_spanned(
                    &branches.last().expect("just pushed").body,
                    "expected `,` after this branch: only a body ending in a block may omit it, \
                     as in a `match` arm",
                ));
            }
        }

        if branches.is_empty() {
            return Err(syn::Error::new(
                Span::call_site(),
                "select! needs at least one branch, written `pattern = future => body`",
            ));
        }

        Ok(Select {
            biased,
            krate,
            branches,
        })
    }
}

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let Select {
        biased,
        krate,
        branches,
    } = match syn::parse2(input) {
        Ok(parsed) => parsed,
        Err(err) => return err.to_compile_error(),
    };

    let count = branches.len();
    // Prefixed so nothing at the call site can collide with them.
    let futures: Vec<Ident> = (0..count)
        .map(|i| Ident::new(&format!("__glommio_select_f{i}"), Span::call_site()))
        .collect();
    let outputs: Vec<Ident> = (0..count)
        .map(|i| Ident::new(&format!("__glommio_select_o{i}"), Span::call_site()))
        .collect();

    let pin = branches.iter().zip(&futures).map(|(branch, name)| {
        let future = &branch.future;
        quote! { let mut #name = ::core::pin::pin!(#future); }
    });

    let declare = outputs.iter().map(|name| {
        quote! { let mut #name = ::core::option::Option::None; }
    });

    // One arm per branch, selected by the rotated offset. Written as a match
    // on the index so the futures keep their distinct types.
    let poll_arms = (0..count).map(|i| {
        let future = &futures[i];
        let output = &outputs[i];
        let index = syn::Index::from(i);
        quote! {
            #index => {
                if let ::core::task::Poll::Ready(value) =
                    ::core::future::Future::poll(#future.as_mut(), __glommio_select_cx)
                {
                    #output = ::core::option::Option::Some(value);
                    return ::core::task::Poll::Ready(#index);
                }
            }
        }
    });

    let handlers = branches.iter().enumerate().map(|(i, branch)| {
        let output = &outputs[i];
        let pattern = &branch.pattern;
        let body = &branch.body;
        let index = syn::Index::from(i);
        quote! {
            #index => {
                let #pattern = #output
                    .take()
                    .expect("select! recorded a branch without its output");
                #body
            }
        }
    });

    // `biased` starts at zero every time; otherwise the start rotates, so a
    // branch that is always ready cannot starve the ones after it. With one
    // branch there is nothing to rotate between, and the loop and modulo a
    // general expansion would emit are degenerate.
    let single = count == 1;

    let start = if biased || single {
        quote! { 0usize }
    } else {
        quote! { #krate::__private::next_select_start() }
    };

    let poll_arms: Vec<_> = poll_arms.collect();
    let poll_body = if single {
        let only = &poll_arms[0];
        quote! {
            match 0usize {
                #only
                _ => ::core::unreachable!(),
            }
        }
    } else {
        quote! {
            for __glommio_select_step in 0..#count {
                match (__glommio_select_start
                    .wrapping_add(__glommio_select_step))
                    % #count
                {
                    #(#poll_arms)*
                    _ => ::core::unreachable!(),
                }
            }
        }
    };

    quote! {{
        #(#declare)*

        let __glommio_select_start = #start;

        // The futures live only for this block, so they are dropped -- and
        // whatever they borrowed is released -- before any handler runs. tokio
        // does the same, and code written against it will not compile
        // otherwise: a handler commonly touches what its own branch borrowed.
        //
        // The futures are polled here but the handlers are not: a handler may
        // await, and a non-async closure cannot. So this reports which branch
        // fired and stashes its output; the handler runs afterwards, in the
        // caller's async context.
        let __glommio_select_which = {
            #(#pin)*

            #krate::__private::poll_fn(
            |__glommio_select_cx| {
                #poll_body

                ::core::task::Poll::Pending
            },
            )
            .await
        };

        match __glommio_select_which {
            #(#handlers)*
            _ => ::core::unreachable!(),
        }
    }}
}
